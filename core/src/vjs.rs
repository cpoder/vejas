// VejasScript — embedded interpreter.
//
// The native flow language of Vejas, descended from WmScript
// (github.com/cpoder/wmscript) and reimplemented in Rust, in-process.
// Design goals, in order: readable on one screen, safely editable from the
// dashboard (no imports, no I/O, no clock — the only side effects are `emit`
// and `invoke`), statically analyzable (business surface, pipeline graph and
// service composition are exact), instantly reloadable.
//
// Language:
//   source "vx.subject"                     flow input declaration (flows only)
//   NAME = <literal>                        business surface (tables/constants)
//   x = expr / a.b = expr                   assignments (pipeline variables)
//   if expr: / elif expr: / else: / end     blocks close with `end`
//   for x in expr: ... end
//   emit "vx.subject", expr
//   invoke svc(k: v, ...)                   composition, MERGES the service's
//                                           pipeline into the caller (wM-style)
//   x = invoke svc(k: v, ...)               composition, captured as a document
//   expressions: literals, {doc: exprs}, [arrays], f"strings {expr}",
//     a.b.c paths, a?.b null-safe, x ?? y, == != < <= > >= in, and/or/not,
//     + - * /, doc[key], orders[].id projection, orders[status == "active"],
//     builtins: upper lower trim len str num split join replace round abs
//
// Pipeline model is webMethods': the incoming event's top-level fields ARE
// the variable space (`event` also holds the whole document). An invoked
// service starts with exactly its named arguments as pipeline and returns
// its whole final pipeline.

use std::collections::HashMap;
use std::path::PathBuf;

use serde_json::{Map, Number, Value};

// ───────────────────────────── lexer ─────────────────────────────

#[derive(Clone, Debug, PartialEq)]
pub enum Tok {
    Ident(String),
    Str(String),
    FStr(Vec<FPart>),
    Num(f64),
    True,
    False,
    Null,
    If,
    Elif,
    Else,
    End,
    For,
    In,
    And,
    Or,
    Not,
    Emit,
    Invoke,
    Source,
    Tool,
    Newline,
    Colon,
    Comma,
    Dot,
    QDot,
    QQ,
    LBrace,
    RBrace,
    LBrack,
    RBrack,
    LParen,
    RParen,
    Eq,
    EqEq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Plus,
    Minus,
    Star,
    Slash,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FPart {
    Lit(String),
    Expr(Vec<Spanned>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Spanned {
    pub tok: Tok,
    pub pos: usize,
}

fn err_at(pos: usize, src: &str, msg: &str) -> String {
    let line = src[..pos.min(src.len())].bytes().filter(|&b| b == b'\n').count() + 1;
    format!("line {line}: {msg}")
}

pub fn lex(src: &str) -> Result<Vec<Spanned>, String> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i] as char;
        match c {
            ' ' | '\t' | '\r' => i += 1,
            '\n' => {
                out.push(Spanned { tok: Tok::Newline, pos: i });
                i += 1;
            }
            '#' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            '"' => {
                let (s, ni) = lex_string(src, i)?;
                out.push(Spanned { tok: Tok::Str(s), pos: i });
                i = ni;
            }
            'f' if i + 1 < b.len() && b[i + 1] == b'"' => {
                let (parts, ni) = lex_fstring(src, i + 1)?;
                out.push(Spanned { tok: Tok::FStr(parts), pos: i });
                i = ni;
            }
            '0'..='9' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
                    i += 1;
                }
                let n: f64 = src[start..i]
                    .parse()
                    .map_err(|_| err_at(start, src, "bad number"))?;
                out.push(Spanned { tok: Tok::Num(n), pos: start });
            }
            'a'..='z' | 'A'..='Z' | '_' => {
                let start = i;
                while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                    i += 1;
                }
                let w = &src[start..i];
                let tok = match w {
                    "if" => Tok::If,
                    "elif" => Tok::Elif,
                    "else" => Tok::Else,
                    "end" => Tok::End,
                    "for" => Tok::For,
                    "in" => Tok::In,
                    "and" => Tok::And,
                    "or" => Tok::Or,
                    "not" => Tok::Not,
                    "emit" => Tok::Emit,
                    "invoke" => Tok::Invoke,
                    "source" => Tok::Source,
                    "tool" => Tok::Tool,
                    "true" => Tok::True,
                    "false" => Tok::False,
                    "null" => Tok::Null,
                    _ => Tok::Ident(w.to_string()),
                };
                out.push(Spanned { tok, pos: start });
            }
            _ => {
                let two = if i + 1 < b.len() { &src[i..i + 2] } else { "" };
                let (tok, len) = match two {
                    "?." => (Tok::QDot, 2),
                    "??" => (Tok::QQ, 2),
                    "==" => (Tok::EqEq, 2),
                    "!=" => (Tok::Ne, 2),
                    "<=" => (Tok::Le, 2),
                    ">=" => (Tok::Ge, 2),
                    _ => match c {
                        ':' => (Tok::Colon, 1),
                        ',' => (Tok::Comma, 1),
                        '.' => (Tok::Dot, 1),
                        '{' => (Tok::LBrace, 1),
                        '}' => (Tok::RBrace, 1),
                        '[' => (Tok::LBrack, 1),
                        ']' => (Tok::RBrack, 1),
                        '(' => (Tok::LParen, 1),
                        ')' => (Tok::RParen, 1),
                        '=' => (Tok::Eq, 1),
                        '<' => (Tok::Lt, 1),
                        '>' => (Tok::Gt, 1),
                        '+' => (Tok::Plus, 1),
                        '-' => (Tok::Minus, 1),
                        '*' => (Tok::Star, 1),
                        '/' => (Tok::Slash, 1),
                        _ => return Err(err_at(i, src, &format!("unexpected character {c:?}"))),
                    },
                };
                out.push(Spanned { tok, pos: i });
                i += len;
            }
        }
    }
    out.push(Spanned { tok: Tok::Newline, pos: b.len() });
    Ok(out)
}

fn lex_string(src: &str, start: usize) -> Result<(String, usize), String> {
    let b = src.as_bytes();
    let mut s: Vec<u8> = Vec::new();
    let mut i = start + 1;
    while i < b.len() {
        match b[i] {
            b'"' => return Ok((String::from_utf8_lossy(&s).into_owned(), i + 1)),
            b'\\' if i + 1 < b.len() => {
                s.push(match b[i + 1] {
                    b'n' => b'\n',
                    b't' => b'\t',
                    other => other,
                });
                i += 2;
            }
            other => {
                s.push(other);
                i += 1;
            }
        }
    }
    Err(err_at(start, src, "unterminated string"))
}

fn lex_fstring(src: &str, quote: usize) -> Result<(Vec<FPart>, usize), String> {
    let b = src.as_bytes();
    let mut parts = Vec::new();
    let mut lit: Vec<u8> = Vec::new();
    let flush = |lit: &mut Vec<u8>, parts: &mut Vec<FPart>| {
        if !lit.is_empty() {
            parts.push(FPart::Lit(String::from_utf8_lossy(lit).into_owned()));
            lit.clear();
        }
    };
    let mut i = quote + 1;
    while i < b.len() {
        match b[i] {
            b'"' => {
                flush(&mut lit, &mut parts);
                return Ok((parts, i + 1));
            }
            b'{' if i + 1 < b.len() && b[i + 1] == b'{' => {
                lit.push(b'{');
                i += 2;
            }
            b'}' if i + 1 < b.len() && b[i + 1] == b'}' => {
                lit.push(b'}');
                i += 2;
            }
            b'{' => {
                flush(&mut lit, &mut parts);
                let mut depth = 1;
                let start = i + 1;
                let mut j = start;
                while j < b.len() && depth > 0 {
                    match b[j] {
                        b'{' => depth += 1,
                        b'}' => depth -= 1,
                        _ => {}
                    }
                    j += 1;
                }
                if depth != 0 {
                    return Err(err_at(i, src, "unterminated { in f-string"));
                }
                let inner = lex(&src[start..j - 1])?;
                parts.push(FPart::Expr(inner));
                i = j;
            }
            b'\\' if i + 1 < b.len() => {
                lit.push(match b[i + 1] {
                    b'n' => b'\n',
                    b't' => b'\t',
                    other => other,
                });
                i += 2;
            }
            other => {
                lit.push(other);
                i += 1;
            }
        }
    }
    Err(err_at(quote, src, "unterminated f-string"))
}

// ───────────────────────────── ast ─────────────────────────────

#[derive(Clone, Debug)]
pub enum Expr {
    Lit(Value),
    FString(Vec<FStringPart>),
    Var(String),
    Doc(Vec<(String, Expr)>),
    Array(Vec<Expr>),
    Field(Box<Expr>, String, bool),
    Index(Box<Expr>, Box<Expr>),
    Projection(Box<Expr>),
    Filter(Box<Expr>, Box<Expr>),
    Call(String, Vec<Expr>),
    Invoke(String, Vec<(String, Expr)>),
    Unary(UnOp, Box<Expr>),
    Bin(BinOp, Box<Expr>, Box<Expr>),
}

#[derive(Clone, Debug)]
pub enum FStringPart {
    Lit(String),
    Expr(Expr),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum UnOp {
    Not,
    Neg,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BinOp {
    Or,
    And,
    Coalesce,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Clone, Debug)]
pub enum Stmt {
    Assign(Vec<String>, Expr),
    If(Vec<(Expr, Vec<Stmt>)>, Option<Vec<Stmt>>),
    For(String, Expr, Vec<Stmt>),
    Emit(Expr, Expr),
    InvokeMerge(String, Vec<(String, Expr)>),
}

pub struct SurfaceEntry {
    pub name: String,
    pub value: Value,
    pub value_span: (usize, usize),
    pub entry_spans: Vec<(String, (usize, usize))>,
}

pub struct Program {
    pub source: Option<String>,
    /// When set, this flow/service is exposed as an MCP tool (and as an API):
    /// `tool "one-line description of what calling it does"`.
    pub tool: Option<String>,
    pub stmts: Vec<Stmt>,
    pub surface: Vec<SurfaceEntry>,
    pub emit_subjects: Vec<String>,
    pub invokes: Vec<String>,
}

// ───────────────────────────── parser ─────────────────────────────

struct Parser<'a> {
    toks: &'a [Spanned],
    i: usize,
    src: &'a str,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> &Tok {
        self.toks.get(self.i).map(|s| &s.tok).unwrap_or(&Tok::Newline)
    }
    fn peek2(&self) -> Option<&Tok> {
        self.toks.get(self.i + 1).map(|s| &s.tok)
    }
    fn pos(&self) -> usize {
        self.toks
            .get(self.i)
            .map(|s| s.pos)
            .unwrap_or_else(|| self.toks.last().map(|s| s.pos).unwrap_or(0))
    }
    fn next(&mut self) -> Tok {
        let t = self
            .toks
            .get(self.i)
            .map(|s| s.tok.clone())
            .unwrap_or(Tok::Newline);
        self.i += 1;
        t
    }
    fn err(&self, msg: &str) -> String {
        err_at(self.pos(), self.src, msg)
    }
    fn eat(&mut self, t: Tok, what: &str) -> Result<(), String> {
        if *self.peek() == t {
            self.i += 1;
            Ok(())
        } else {
            Err(self.err(&format!("expected {what}, found {:?}", self.peek())))
        }
    }
    fn skip_newlines(&mut self) {
        while self.i < self.toks.len() && *self.peek() == Tok::Newline {
            self.i += 1;
        }
    }
    fn at_end(&self) -> bool {
        self.i >= self.toks.len() - 1
    }

    fn program(&mut self) -> Result<Program, String> {
        let mut source = None;
        let mut tool = None;
        let mut stmts = Vec::new();
        let mut surface = Vec::new();
        loop {
            self.skip_newlines();
            if self.at_end() {
                break;
            }
            if *self.peek() == Tok::Source {
                self.next();
                match self.next() {
                    Tok::Str(s) => source = Some(s),
                    _ => return Err(self.err("source expects a string subject")),
                }
                continue;
            }
            if *self.peek() == Tok::Tool {
                self.next();
                match self.next() {
                    Tok::Str(s) => tool = Some(s),
                    _ => return Err(self.err("tool expects a string description")),
                }
                continue;
            }
            if let Tok::Ident(name) = self.peek().clone() {
                let uppercase = name
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit())
                    && name.chars().any(|c| c.is_ascii_uppercase());
                if uppercase && self.peek2() == Some(&Tok::Eq) {
                    let save = self.i;
                    self.i += 2;
                    let start = self.pos();
                    if let Some((value, entry_spans, end)) = self.try_literal()? {
                        stmts.push(Stmt::Assign(vec![name.clone()], Expr::Lit(value.clone())));
                        surface.push(SurfaceEntry {
                            name,
                            value,
                            value_span: (start, end),
                            entry_spans,
                        });
                        continue;
                    }
                    self.i = save;
                }
            }
            stmts.push(self.stmt()?);
        }
        let mut prog = Program {
            source,
            tool,
            stmts,
            surface,
            emit_subjects: Vec::new(),
            invokes: Vec::new(),
        };
        let consts: Vec<(String, String)> = prog
            .surface
            .iter()
            .filter_map(|e| e.value.as_str().map(|s| (e.name.clone(), s.to_string())))
            .collect();
        let (mut emits, mut invokes) = (Vec::new(), Vec::new());
        collect_static(&prog.stmts, &consts, &mut emits, &mut invokes);
        prog.emit_subjects = emits;
        prog.invokes = invokes;
        Ok(prog)
    }

    fn try_literal(
        &mut self,
    ) -> Result<Option<(Value, Vec<(String, (usize, usize))>, usize)>, String> {
        let save = self.i;
        match self.literal_value() {
            Ok((v, spans, end)) => match self.peek() {
                Tok::Newline => Ok(Some((v, spans, end))),
                _ => {
                    self.i = save;
                    Ok(None)
                }
            },
            Err(_) => {
                self.i = save;
                Ok(None)
            }
        }
    }

    /// Parse a pure literal, returning (value, doc entry spans, end byte).
    /// The end byte is the position of the token FOLLOWING the literal, so
    /// spans are trimmed to the literal's own text by the caller using the
    /// recorded positions of the next token; for in-place editing this is
    /// good enough because we splice [start..end_of_literal_token_next).
    fn literal_value(&mut self) -> Result<(Value, Vec<(String, (usize, usize))>, usize), String> {
        match self.next() {
            Tok::Str(_) | Tok::Num(_) | Tok::True | Tok::False | Tok::Null => {
                let v = match &self.toks[self.i - 1].tok {
                    Tok::Str(s) => Value::String(s.clone()),
                    Tok::Num(n) => num(*n),
                    Tok::True => Value::Bool(true),
                    Tok::False => Value::Bool(false),
                    _ => Value::Null,
                };
                Ok((v, vec![], self.pos()))
            }
            Tok::Minus => match self.next() {
                Tok::Num(n) => Ok((num(-n), vec![], self.pos())),
                _ => Err(self.err("expected number")),
            },
            Tok::LBrack => {
                let mut items = Vec::new();
                self.skip_newlines();
                while *self.peek() != Tok::RBrack {
                    let (v, _, _) = self.literal_value()?;
                    items.push(v);
                    self.skip_newlines();
                    if *self.peek() == Tok::Comma {
                        self.next();
                        self.skip_newlines();
                    }
                }
                self.next();
                Ok((Value::Array(items), vec![], self.pos()))
            }
            Tok::LBrace => {
                let mut map = Map::new();
                let mut spans = Vec::new();
                self.skip_newlines();
                while *self.peek() != Tok::RBrace {
                    let key = match self.next() {
                        Tok::Ident(k) | Tok::Str(k) => k,
                        _ => return Err(self.err("expected key")),
                    };
                    self.eat(Tok::Colon, ":")?;
                    let vstart = self.pos();
                    let (v, _, _) = self.literal_value()?;
                    // the value's text ends where the *current* token starts,
                    // trimmed of trailing whitespace/comma by the splicer
                    let vend = self.pos();
                    spans.push((key.clone(), (vstart, vend)));
                    map.insert(key, v);
                    self.skip_newlines();
                    if *self.peek() == Tok::Comma {
                        self.next();
                        self.skip_newlines();
                    }
                }
                self.next();
                Ok((Value::Object(map), spans, self.pos()))
            }
            _ => Err(self.err("not a literal")),
        }
    }

    fn block(&mut self) -> Result<Vec<Stmt>, String> {
        let mut stmts = Vec::new();
        loop {
            self.skip_newlines();
            match self.peek() {
                Tok::End | Tok::Elif | Tok::Else => break,
                _ => {}
            }
            if self.at_end() {
                return Err(self.err("missing `end`"));
            }
            stmts.push(self.stmt()?);
        }
        Ok(stmts)
    }

    fn invoke_args(&mut self) -> Result<(String, Vec<(String, Expr)>), String> {
        let mut name = match self.next() {
            Tok::Ident(n) => n,
            _ => return Err(self.err("expected service name after invoke")),
        };
        // qualified reference: invoke pkg:service(...)
        if *self.peek() == Tok::Colon {
            if let Some(Tok::Ident(_)) = self.peek2() {
                self.next();
                if let Tok::Ident(svc) = self.next() {
                    name = format!("{name}:{svc}");
                }
            }
        }
        self.eat(Tok::LParen, "(")?;
        let mut args = Vec::new();
        while *self.peek() != Tok::RParen {
            let key = match self.next() {
                Tok::Ident(k) => k,
                _ => return Err(self.err("expected argument name")),
            };
            self.eat(Tok::Colon, ":")?;
            args.push((key, self.expr(0)?));
            if *self.peek() == Tok::Comma {
                self.next();
            }
        }
        self.next();
        Ok((name, args))
    }

    fn stmt(&mut self) -> Result<Stmt, String> {
        match self.peek().clone() {
            Tok::Emit => {
                self.next();
                let subject = self.expr(0)?;
                self.eat(Tok::Comma, ",")?;
                let payload = self.expr(0)?;
                Ok(Stmt::Emit(subject, payload))
            }
            Tok::Invoke => {
                self.next();
                let (name, args) = self.invoke_args()?;
                Ok(Stmt::InvokeMerge(name, args))
            }
            Tok::If => {
                self.next();
                let mut arms = Vec::new();
                let cond = self.expr(0)?;
                self.eat(Tok::Colon, ":")?;
                arms.push((cond, self.block()?));
                let mut otherwise = None;
                loop {
                    match self.peek() {
                        Tok::Elif => {
                            self.next();
                            let c = self.expr(0)?;
                            self.eat(Tok::Colon, ":")?;
                            arms.push((c, self.block()?));
                        }
                        Tok::Else => {
                            self.next();
                            self.eat(Tok::Colon, ":")?;
                            otherwise = Some(self.block()?);
                        }
                        Tok::End => {
                            self.next();
                            break;
                        }
                        _ => return Err(self.err("expected elif/else/end")),
                    }
                }
                Ok(Stmt::If(arms, otherwise))
            }
            Tok::For => {
                self.next();
                let var = match self.next() {
                    Tok::Ident(v) => v,
                    _ => return Err(self.err("expected loop variable")),
                };
                self.eat(Tok::In, "in")?;
                let iter = self.expr(0)?;
                self.eat(Tok::Colon, ":")?;
                let body = self.block()?;
                self.eat(Tok::End, "end")?;
                Ok(Stmt::For(var, iter, body))
            }
            Tok::Ident(first) => {
                self.next();
                let mut path = vec![first];
                while *self.peek() == Tok::Dot {
                    self.next();
                    match self.next() {
                        Tok::Ident(p) => path.push(p),
                        _ => return Err(self.err("expected field name")),
                    }
                }
                self.eat(Tok::Eq, "=")?;
                if *self.peek() == Tok::Invoke {
                    self.next();
                    let (name, args) = self.invoke_args()?;
                    return Ok(Stmt::Assign(path, Expr::Invoke(name, args)));
                }
                let value = self.expr(0)?;
                Ok(Stmt::Assign(path, value))
            }
            _ => Err(self.err("expected a statement")),
        }
    }

    fn expr(&mut self, min_bp: u8) -> Result<Expr, String> {
        let mut lhs = match self.next() {
            Tok::Num(n) => Expr::Lit(num(n)),
            Tok::Str(s) => Expr::Lit(Value::String(s)),
            Tok::True => Expr::Lit(Value::Bool(true)),
            Tok::False => Expr::Lit(Value::Bool(false)),
            Tok::Null => Expr::Lit(Value::Null),
            Tok::FStr(parts) => {
                let mut out = Vec::new();
                for p in parts {
                    match p {
                        FPart::Lit(s) => out.push(FStringPart::Lit(s)),
                        FPart::Expr(toks) => {
                            let mut sub = Parser { toks: &toks, i: 0, src: self.src };
                            out.push(FStringPart::Expr(sub.expr(0)?));
                        }
                    }
                }
                Expr::FString(out)
            }
            Tok::Not => Expr::Unary(UnOp::Not, Box::new(self.expr(9)?)),
            Tok::Minus => Expr::Unary(UnOp::Neg, Box::new(self.expr(9)?)),
            Tok::Invoke => {
                let (name, args) = self.invoke_args()?;
                Expr::Invoke(name, args)
            }
            Tok::LParen => {
                let e = self.expr(0)?;
                self.eat(Tok::RParen, ")")?;
                e
            }
            Tok::LBrack => {
                let mut items = Vec::new();
                self.skip_newlines();
                while *self.peek() != Tok::RBrack {
                    items.push(self.expr(0)?);
                    self.skip_newlines();
                    if *self.peek() == Tok::Comma {
                        self.next();
                        self.skip_newlines();
                    }
                }
                self.next();
                Expr::Array(items)
            }
            Tok::LBrace => {
                let mut fields = Vec::new();
                self.skip_newlines();
                while *self.peek() != Tok::RBrace {
                    let key = match self.next() {
                        Tok::Ident(k) | Tok::Str(k) => k,
                        _ => return Err(self.err("expected key")),
                    };
                    self.eat(Tok::Colon, ":")?;
                    fields.push((key, self.expr(0)?));
                    self.skip_newlines();
                    if *self.peek() == Tok::Comma {
                        self.next();
                        self.skip_newlines();
                    }
                }
                self.next();
                Expr::Doc(fields)
            }
            Tok::Ident(name) => {
                if *self.peek() == Tok::LParen {
                    self.next();
                    let mut args = Vec::new();
                    while *self.peek() != Tok::RParen {
                        args.push(self.expr(0)?);
                        if *self.peek() == Tok::Comma {
                            self.next();
                        }
                    }
                    self.next();
                    Expr::Call(name, args)
                } else {
                    Expr::Var(name)
                }
            }
            other => return Err(self.err(&format!("unexpected {other:?} in expression"))),
        };

        loop {
            match self.peek() {
                Tok::Dot => {
                    self.next();
                    match self.next() {
                        Tok::Ident(f) => lhs = Expr::Field(Box::new(lhs), f, false),
                        _ => return Err(self.err("expected field name")),
                    }
                }
                Tok::QDot => {
                    self.next();
                    match self.next() {
                        Tok::Ident(f) => lhs = Expr::Field(Box::new(lhs), f, true),
                        _ => return Err(self.err("expected field name")),
                    }
                }
                Tok::LBrack => {
                    self.next();
                    if *self.peek() == Tok::RBrack {
                        self.next();
                        lhs = Expr::Projection(Box::new(lhs));
                    } else {
                        let inner = self.expr(0)?;
                        self.eat(Tok::RBrack, "]")?;
                        let is_filter = matches!(
                            inner,
                            Expr::Bin(
                                BinOp::Eq
                                    | BinOp::Ne
                                    | BinOp::Lt
                                    | BinOp::Le
                                    | BinOp::Gt
                                    | BinOp::Ge
                                    | BinOp::In
                                    | BinOp::And
                                    | BinOp::Or,
                                _,
                                _
                            ) | Expr::Unary(UnOp::Not, _)
                        );
                        lhs = if is_filter {
                            Expr::Filter(Box::new(lhs), Box::new(inner))
                        } else {
                            Expr::Index(Box::new(lhs), Box::new(inner))
                        };
                    }
                }
                _ => break,
            }
        }

        loop {
            let (op, bp) = match self.peek() {
                Tok::Or => (BinOp::Or, 1),
                Tok::And => (BinOp::And, 2),
                Tok::QQ => (BinOp::Coalesce, 3),
                Tok::EqEq => (BinOp::Eq, 4),
                Tok::Ne => (BinOp::Ne, 4),
                Tok::Lt => (BinOp::Lt, 4),
                Tok::Le => (BinOp::Le, 4),
                Tok::Gt => (BinOp::Gt, 4),
                Tok::Ge => (BinOp::Ge, 4),
                Tok::In => (BinOp::In, 4),
                Tok::Plus => (BinOp::Add, 5),
                Tok::Minus => (BinOp::Sub, 5),
                Tok::Star => (BinOp::Mul, 6),
                Tok::Slash => (BinOp::Div, 6),
                _ => break,
            };
            if bp <= min_bp {
                break;
            }
            self.next();
            let rhs = self.expr(bp)?;
            lhs = Expr::Bin(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }
}

fn collect_static(
    stmts: &[Stmt],
    consts: &[(String, String)],
    emits: &mut Vec<String>,
    invokes: &mut Vec<String>,
) {
    fn push(v: &mut Vec<String>, s: String) {
        if !v.contains(&s) {
            v.push(s);
        }
    }
    for s in stmts {
        match s {
            Stmt::Emit(subj, _) => match subj {
                Expr::Lit(Value::String(s)) => push(emits, s.clone()),
                Expr::Var(name) => {
                    if let Some((_, v)) = consts.iter().find(|(n, _)| n == name) {
                        push(emits, v.clone());
                    }
                }
                _ => {}
            },
            Stmt::InvokeMerge(name, _) => push(invokes, name.clone()),
            Stmt::Assign(_, Expr::Invoke(name, _)) => push(invokes, name.clone()),
            Stmt::If(arms, otherwise) => {
                for (_, b) in arms {
                    collect_static(b, consts, emits, invokes);
                }
                if let Some(b) = otherwise {
                    collect_static(b, consts, emits, invokes);
                }
            }
            Stmt::For(_, _, b) => collect_static(b, consts, emits, invokes),
            _ => {}
        }
    }
}

pub fn parse(src: &str) -> Result<Program, String> {
    let toks = lex(src)?;
    let mut p = Parser { toks: &toks, i: 0, src };
    p.program()
}

// ───────────────────────────── eval ─────────────────────────────

fn num(n: f64) -> Value {
    if n.fract() == 0.0 && n.abs() < 9e15 {
        Value::Number(Number::from(n as i64))
    } else {
        Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null)
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(_) => true,
    }
}

fn to_display(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Loads and caches composed services.
///
/// Resolution (webMethods packages, with a stricter coupling rule):
///   - `invoke fmt(...)`          -> <caller package>/services/fmt.vjs
///   - `invoke billing:fmt(...)`  -> packages/billing/services/fmt.vjs
///     (or the project root when the package is "default")
///   - a cross-package invoke is allowed ONLY if the target package's
///     package.vjs manifest lists the service in EXPORTS = [...]. Private by
///     default: no manifest, or not listed, means denied with a clear error.
///     Between packages, prefer the bus; EXPORTS is the deliberate exception.
pub struct Engine {
    pub project_root: PathBuf,
    pub caller_pkg: String,
    cache: HashMap<String, Program>,
    exports: HashMap<String, Vec<String>>,
    depth: usize,
}

impl Engine {
    pub fn new(project_root: PathBuf, caller_pkg: String) -> Self {
        Engine {
            project_root,
            caller_pkg,
            cache: HashMap::new(),
            exports: HashMap::new(),
            depth: 0,
        }
    }
    pub fn invalidate(&mut self) {
        self.cache.clear();
        self.exports.clear();
    }
    fn pkg_dir(&self, pkg: &str) -> PathBuf {
        if pkg == "default" {
            self.project_root.clone()
        } else {
            self.project_root.join("packages").join(pkg)
        }
    }
    fn exports_of(&mut self, pkg: &str) -> Result<Vec<String>, String> {
        if let Some(e) = self.exports.get(pkg) {
            return Ok(e.clone());
        }
        let manifest = self.pkg_dir(pkg).join("package.vjs");
        let src = std::fs::read_to_string(&manifest).map_err(|_| {
            format!(
                "package {pkg:?} has no package.vjs manifest; cross-package invoke requires an EXPORTS list"
            )
        })?;
        let prog = parse(&src).map_err(|e| format!("package {pkg} manifest: {e}"))?;
        let list = prog
            .surface
            .iter()
            .find(|e| e.name == "EXPORTS")
            .map(|e| {
                e.value
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        self.exports.insert(pkg.to_string(), list.clone());
        Ok(list)
    }
    /// Resolve an invoke target to (package, service) and enforce EXPORTS.
    fn resolve(&mut self, name: &str) -> Result<(String, String), String> {
        let (pkg, svc) = match name.split_once(':') {
            Some((p, s)) => (p.to_string(), s.to_string()),
            None => (self.caller_pkg.clone(), name.to_string()),
        };
        let valid = |s: &str| {
            !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        };
        if !valid(&pkg) || !valid(&svc) {
            return Err(format!("invalid service reference {name:?}"));
        }
        if pkg != self.caller_pkg {
            let exports = self.exports_of(&pkg)?;
            if !exports.iter().any(|e| e == &svc) {
                return Err(format!(
                    "service {pkg}:{svc} is not exported by package {pkg:?} (add {svc:?} to EXPORTS in its package.vjs, or talk to it over the bus)"
                ));
            }
        }
        Ok((pkg, svc))
    }
    fn service_stmts(&mut self, pkg: &str, svc: &str) -> Result<Vec<Stmt>, String> {
        let key = format!("{pkg}:{svc}");
        if !self.cache.contains_key(&key) {
            let path = self.pkg_dir(pkg).join("services").join(format!("{svc}.vjs"));
            let src = std::fs::read_to_string(&path)
                .map_err(|_| format!("unknown service {key:?} (no {})", path.display()))?;
            let prog = parse(&src).map_err(|e| format!("service {key}: {e}"))?;
            self.cache.insert(key.clone(), prog);
        }
        Ok(self.cache.get(&key).unwrap().stmts.clone())
    }
}

pub struct Ctx {
    pub vars: Map<String, Value>,
    pub emits: Vec<(String, Value)>,
}

pub fn run(prog: &Program, event: &Value, engine: &mut Engine) -> Result<Ctx, String> {
    let mut vars = Map::new();
    if let Value::Object(obj) = event {
        for (k, v) in obj {
            vars.insert(k.clone(), v.clone());
        }
    }
    vars.insert("event".into(), event.clone());
    let mut ctx = Ctx { vars, emits: Vec::new() };
    exec_block(&prog.stmts, &mut ctx, engine)?;
    Ok(ctx)
}

fn run_service(
    name: &str,
    args: Map<String, Value>,
    engine: &mut Engine,
    emits: &mut Vec<(String, Value)>,
) -> Result<Value, String> {
    if engine.depth >= 32 {
        return Err(format!("invoke depth exceeded at {name}"));
    }
    let (pkg, svc) = engine.resolve(name)?;
    engine.depth += 1;
    let stmts = match engine.service_stmts(&pkg, &svc) {
        Ok(s) => s,
        Err(e) => {
            engine.depth -= 1;
            return Err(e);
        }
    };
    // The invoked service's own invokes resolve within ITS package.
    let saved_pkg = std::mem::replace(&mut engine.caller_pkg, pkg);
    let mut ctx = Ctx { vars: args.clone(), emits: Vec::new() };
    ctx.vars.insert("event".into(), Value::Object(args));
    let result = exec_block(&stmts, &mut ctx, engine);
    engine.caller_pkg = saved_pkg;
    engine.depth -= 1;
    result?;
    emits.append(&mut ctx.emits);
    ctx.vars.remove("event");
    Ok(Value::Object(ctx.vars))
}

fn exec_block(stmts: &[Stmt], ctx: &mut Ctx, engine: &mut Engine) -> Result<(), String> {
    for s in stmts {
        match s {
            Stmt::Assign(path, e) => {
                let v = eval(e, ctx, engine)?;
                if path.len() == 1 {
                    ctx.vars.insert(path[0].clone(), v);
                } else {
                    let root = ctx
                        .vars
                        .entry(path[0].clone())
                        .or_insert_with(|| Value::Object(Map::new()));
                    set_path(root, &path[1..], v);
                }
            }
            Stmt::InvokeMerge(name, args) => {
                let mut arg_map = Map::new();
                for (k, e) in args {
                    arg_map.insert(k.clone(), eval(e, ctx, engine)?);
                }
                let mut emits = Vec::new();
                let out = run_service(name, arg_map, engine, &mut emits)?;
                ctx.emits.append(&mut emits);
                if let Value::Object(o) = out {
                    for (k, v) in o {
                        ctx.vars.insert(k, v);
                    }
                }
            }
            Stmt::If(arms, otherwise) => {
                let mut done = false;
                for (cond, body) in arms {
                    if truthy(&eval(cond, ctx, engine)?) {
                        exec_block(body, ctx, engine)?;
                        done = true;
                        break;
                    }
                }
                if !done {
                    if let Some(body) = otherwise {
                        exec_block(body, ctx, engine)?;
                    }
                }
            }
            Stmt::For(var, iter, body) => {
                let items = eval(iter, ctx, engine)?;
                if let Value::Array(items) = items {
                    for item in items {
                        ctx.vars.insert(var.clone(), item);
                        exec_block(body, ctx, engine)?;
                    }
                }
            }
            Stmt::Emit(subj, payload) => {
                let s = match eval(subj, ctx, engine)? {
                    Value::String(s) => s,
                    other => return Err(format!("emit subject must be a string, got {other}")),
                };
                let p = eval(payload, ctx, engine)?;
                ctx.emits.push((s, p));
            }
        }
    }
    Ok(())
}

fn set_path(root: &mut Value, path: &[String], v: Value) {
    if !root.is_object() {
        *root = Value::Object(Map::new());
    }
    let obj = root.as_object_mut().unwrap();
    if path.len() == 1 {
        obj.insert(path[0].clone(), v);
    } else {
        let next = obj
            .entry(path[0].clone())
            .or_insert_with(|| Value::Object(Map::new()));
        set_path(next, &path[1..], v);
    }
}

fn eval(e: &Expr, ctx: &mut Ctx, engine: &mut Engine) -> Result<Value, String> {
    Ok(match e {
        Expr::Lit(v) => v.clone(),
        Expr::Var(name) => ctx.vars.get(name).cloned().unwrap_or(Value::Null),
        Expr::FString(parts) => {
            let mut s = String::new();
            for p in parts {
                match p {
                    FStringPart::Lit(l) => s.push_str(l),
                    FStringPart::Expr(e) => s.push_str(&to_display(&eval(e, ctx, engine)?)),
                }
            }
            Value::String(s)
        }
        Expr::Doc(fields) => {
            let mut m = Map::new();
            for (k, v) in fields {
                m.insert(k.clone(), eval(v, ctx, engine)?);
            }
            Value::Object(m)
        }
        Expr::Array(items) => Value::Array(
            items
                .iter()
                .map(|i| eval(i, ctx, engine))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expr::Invoke(name, args) => {
            let mut arg_map = Map::new();
            for (k, e) in args {
                arg_map.insert(k.clone(), eval(e, ctx, engine)?);
            }
            let mut emits = Vec::new();
            let out = run_service(name, arg_map, engine, &mut emits)?;
            ctx.emits.append(&mut emits);
            out
        }
        Expr::Field(base, f, safe) => {
            let b = eval(base, ctx, engine)?;
            match &b {
                Value::Object(o) => o.get(f).cloned().unwrap_or(Value::Null),
                Value::Array(items) => Value::Array(
                    items
                        .iter()
                        .map(|it| it.get(f).cloned().unwrap_or(Value::Null))
                        .collect(),
                ),
                Value::Null => Value::Null,
                _ if *safe => Value::Null,
                other => return Err(format!("cannot read field {f:?} of {other}")),
            }
        }
        Expr::Index(base, key) => {
            let b = eval(base, ctx, engine)?;
            let k = eval(key, ctx, engine)?;
            match (&b, &k) {
                (Value::Object(o), Value::String(s)) => o.get(s).cloned().unwrap_or(Value::Null),
                (Value::Array(a), Value::Number(n)) => {
                    let i = n.as_i64().unwrap_or(-1);
                    if i >= 0 {
                        a.get(i as usize).cloned().unwrap_or(Value::Null)
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            }
        }
        Expr::Projection(base) => eval(base, ctx, engine)?,
        Expr::Filter(base, cond) => {
            let b = eval(base, ctx, engine)?;
            let Value::Array(items) = b else {
                return Ok(Value::Array(vec![]));
            };
            let mut out = Vec::new();
            for item in items {
                let saved: Vec<(String, Option<Value>)> = if let Value::Object(o) = &item {
                    let saved = o
                        .keys()
                        .map(|k| (k.clone(), ctx.vars.get(k).cloned()))
                        .collect();
                    for (k, v) in o {
                        ctx.vars.insert(k.clone(), v.clone());
                    }
                    saved
                } else {
                    vec![]
                };
                let keep = truthy(&eval(cond, ctx, engine)?);
                for (k, old) in saved {
                    match old {
                        Some(v) => ctx.vars.insert(k, v),
                        None => ctx.vars.remove(&k),
                    };
                }
                if keep {
                    out.push(item);
                }
            }
            Value::Array(out)
        }
        Expr::Call(name, args) => {
            let vals: Vec<Value> = args
                .iter()
                .map(|a| eval(a, ctx, engine))
                .collect::<Result<Vec<_>, _>>()?;
            builtin(name, &vals)?
        }
        Expr::Unary(op, e) => {
            let v = eval(e, ctx, engine)?;
            match op {
                UnOp::Not => Value::Bool(!truthy(&v)),
                UnOp::Neg => match v.as_f64() {
                    Some(f) => num(-f),
                    None => Value::Null,
                },
            }
        }
        Expr::Bin(op, l, r) => {
            match op {
                BinOp::And => {
                    let lv = eval(l, ctx, engine)?;
                    return Ok(if truthy(&lv) { eval(r, ctx, engine)? } else { lv });
                }
                BinOp::Or => {
                    let lv = eval(l, ctx, engine)?;
                    return Ok(if truthy(&lv) { lv } else { eval(r, ctx, engine)? });
                }
                BinOp::Coalesce => {
                    let lv = eval(l, ctx, engine)?;
                    return Ok(if lv.is_null() { eval(r, ctx, engine)? } else { lv });
                }
                _ => {}
            }
            let lv = eval(l, ctx, engine)?;
            let rv = eval(r, ctx, engine)?;
            match op {
                BinOp::Eq => Value::Bool(lv == rv),
                BinOp::Ne => Value::Bool(lv != rv),
                BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge => {
                    let ord = match (&lv, &rv) {
                        (Value::Number(a), Value::Number(b)) => a
                            .as_f64()
                            .and_then(|x| b.as_f64().and_then(|y| x.partial_cmp(&y))),
                        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
                        _ => None,
                    };
                    let Some(ord) = ord else {
                        return Ok(Value::Bool(false));
                    };
                    Value::Bool(match op {
                        BinOp::Lt => ord == std::cmp::Ordering::Less,
                        BinOp::Le => ord != std::cmp::Ordering::Greater,
                        BinOp::Gt => ord == std::cmp::Ordering::Greater,
                        _ => ord != std::cmp::Ordering::Less,
                    })
                }
                BinOp::In => match &rv {
                    Value::Array(a) => Value::Bool(a.contains(&lv)),
                    Value::String(s) => match &lv {
                        Value::String(needle) => Value::Bool(s.contains(needle.as_str())),
                        _ => Value::Bool(false),
                    },
                    Value::Object(o) => match &lv {
                        Value::String(k) => Value::Bool(o.contains_key(k)),
                        _ => Value::Bool(false),
                    },
                    _ => Value::Bool(false),
                },
                BinOp::Add => match (&lv, &rv) {
                    (Value::Number(a), Value::Number(b)) => {
                        num(a.as_f64().unwrap_or(0.0) + b.as_f64().unwrap_or(0.0))
                    }
                    (Value::Array(a), Value::Array(b)) => {
                        // array concatenation: how lists are built in a loop
                        let mut out = a.clone();
                        out.extend(b.clone());
                        Value::Array(out)
                    }
                    _ => Value::String(format!("{}{}", to_display(&lv), to_display(&rv))),
                },
                BinOp::Sub | BinOp::Mul | BinOp::Div => match (lv.as_f64(), rv.as_f64()) {
                    (Some(a), Some(b)) => match op {
                        BinOp::Sub => num(a - b),
                        BinOp::Mul => num(a * b),
                        _ => {
                            if b == 0.0 {
                                Value::Null
                            } else {
                                num(a / b)
                            }
                        }
                    },
                    _ => Value::Null,
                },
                _ => unreachable!(),
            }
        }
    })
}

fn builtin(name: &str, args: &[Value]) -> Result<Value, String> {
    let arg = |i: usize| args.get(i).cloned().unwrap_or(Value::Null);
    Ok(match name {
        "upper" => match arg(0) {
            Value::String(s) => Value::String(s.to_uppercase()),
            Value::Null => Value::Null,
            v => Value::String(to_display(&v).to_uppercase()),
        },
        "lower" => match arg(0) {
            Value::String(s) => Value::String(s.to_lowercase()),
            Value::Null => Value::Null,
            v => Value::String(to_display(&v).to_lowercase()),
        },
        "trim" => match arg(0) {
            Value::String(s) => Value::String(s.trim().to_string()),
            v => v,
        },
        "len" => match arg(0) {
            Value::String(s) => num(s.chars().count() as f64),
            Value::Array(a) => num(a.len() as f64),
            Value::Object(o) => num(o.len() as f64),
            _ => num(0.0),
        },
        "str" => Value::String(to_display(&arg(0))),
        "num" => match arg(0) {
            Value::Number(n) => Value::Number(n),
            Value::String(s) => s.trim().parse::<f64>().map(num).unwrap_or(Value::Null),
            Value::Bool(b) => num(if b { 1.0 } else { 0.0 }),
            _ => Value::Null,
        },
        "split" => match (arg(0), arg(1)) {
            (Value::String(s), Value::String(sep)) => Value::Array(
                s.split(sep.as_str())
                    .map(|p| Value::String(p.to_string()))
                    .collect(),
            ),
            _ => Value::Null,
        },
        "join" => match (arg(0), arg(1)) {
            (Value::Array(a), Value::String(sep)) => {
                Value::String(a.iter().map(to_display).collect::<Vec<_>>().join(&sep))
            }
            _ => Value::Null,
        },
        "replace" => match (arg(0), arg(1), arg(2)) {
            (Value::String(s), Value::String(from), Value::String(to)) => {
                Value::String(s.replace(&from, &to))
            }
            _ => Value::Null,
        },
        "round" => match (arg(0).as_f64(), arg(1).as_f64()) {
            (Some(v), Some(d)) => {
                let f = 10f64.powi(d as i32);
                num((v * f).round() / f)
            }
            (Some(v), None) => num(v.round()),
            _ => Value::Null,
        },
        "abs" => arg(0).as_f64().map(|f| num(f.abs())).unwrap_or(Value::Null),
        _ => return Err(format!("unknown function {name}()")),
    })
}

// ───────────────────────────── editing ─────────────────────────────

/// Rewrite one surface literal in place (whole constant with key "-", or one
/// entry of a top-level doc literal). The result must re-parse.
pub fn set_literal(src: &str, name: &str, key: &str, new_value: &Value) -> Result<String, String> {
    let prog = parse(src)?;
    let entry = prog
        .surface
        .iter()
        .find(|e| e.name == name)
        .ok_or_else(|| format!("no literal assignment {name:?}"))?;
    let (start, end) = if key.is_empty() || key == "-" {
        entry.value_span
    } else {
        entry
            .entry_spans
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, span)| *span)
            .ok_or_else(|| format!("{name} has no key {key:?}"))?
    };
    // spans end at the NEXT token's start; trim only trailing whitespace so
    // we replace exactly the literal's own text (separators are next tokens,
    // therefore already excluded)
    let mut end = end.min(src.len());
    while end > start {
        let c = src.as_bytes()[end - 1] as char;
        if c == ' ' || c == '\t' || c == '\r' {
            end -= 1;
        } else {
            break;
        }
    }
    let rendered = serde_json::to_string(new_value).map_err(|e| e.to_string())?;
    let mut out = String::with_capacity(src.len() + rendered.len());
    out.push_str(&src[..start]);
    out.push_str(&rendered);
    out.push_str(&src[end..]);
    parse(&out)?;
    Ok(out)
}

// ───────────────────────────── tests ─────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn eval_flow(src: &str, event: Value) -> Result<Vec<(String, Value)>, String> {
        let prog = parse(src)?;
        let mut engine = Engine::new(std::env::temp_dir(), "default".into());
        run(&prog, &event, &mut engine).map(|c| c.emits)
    }
    fn one_emit(src: &str, event: Value) -> Value {
        let emits = eval_flow(src, event).expect("run");
        assert_eq!(emits.len(), 1, "expected exactly one emit");
        emits[0].1.clone()
    }

    #[test]
    fn fstring_utf8_and_escapes() {
        let p = one_emit(
            "emit \"vx.t\", {r: f\"héllo {upper(name)} — 100 % {{brace}}\"}",
            json!({"name": "josé"}),
        );
        assert_eq!(p["r"], "héllo JOSÉ — 100 % {brace}");
    }

    #[test]
    fn null_safe_and_coalesce() {
        let p = one_emit(
            "emit \"vx.t\", {a: user?.email, b: user?.email ?? \"none\", c: missing ?? 0}",
            json!({}),
        );
        assert_eq!(p["a"], Value::Null);
        assert_eq!(p["b"], "none");
        assert_eq!(p["c"], 0);
    }

    #[test]
    fn projection_and_filter_with_outer_scope() {
        let p = one_emit(
            "MIN = 10\nemit \"vx.t\", {ids: orders[].id, big: orders[total > MIN][].id}",
            json!({"orders": [{"id": "a", "total": 5}, {"id": "b", "total": 50}]}),
        );
        assert_eq!(p["ids"], json!(["a", "b"]));
        assert_eq!(p["big"], json!(["b"]));
    }

    #[test]
    fn index_by_expression_and_table() {
        let p = one_emit(
            "T = {\"fr\": \"France\"}\nemit \"vx.t\", {c: T[code] ?? \"?\", n: xs[1]}",
            json!({"code": "fr", "xs": [10, 20, 30]}),
        );
        assert_eq!(p["c"], "France");
        assert_eq!(p["n"], 20);
    }

    #[test]
    fn arithmetic_division_by_zero_is_null() {
        let p = one_emit(
            "emit \"vx.t\", {a: 7 / 2, b: 1 / 0, c: round(10 / 3, 2)}",
            json!({}),
        );
        assert_eq!(p["a"], 3.5);
        assert_eq!(p["b"], Value::Null);
        assert_eq!(p["c"], 3.33);
    }

    #[test]
    fn array_concat_builds_lists_in_for() {
        let p = one_emit(
            "out = []\nfor l in lines:\n  out = out + [{s: l.sku, q: num(l.q)}]\nend\nemit \"vx.t\", {out: out}",
            json!({"lines": [{"sku": "A", "q": "2"}, {"sku": "B", "q": "3"}]}),
        );
        assert_eq!(p["out"], json!([{"s": "A", "q": 2}, {"s": "B", "q": 3}]));
    }

    #[test]
    fn nested_assignment_builds_documents() {
        let p = one_emit(
            "doc.head.id = 1\ndoc.head.kind = \"x\"\nemit \"vx.t\", doc",
            json!({}),
        );
        assert_eq!(p, json!({"head": {"id": 1, "kind": "x"}}));
    }

    #[test]
    fn in_operator_array_string_object() {
        let p = one_emit(
            "emit \"vx.t\", {a: \"P1\" in codes, b: \"P9\" in codes, c: \"ell\" in \"hello\", d: \"k\" in {\"k\": 1}}",
            json!({"codes": ["P1", "P2"]}),
        );
        assert_eq!(p["a"], true);
        assert_eq!(p["b"], false);
        assert_eq!(p["c"], true);
        assert_eq!(p["d"], true);
    }

    #[test]
    fn builtins_split_join_replace_trim_len() {
        let p = one_emit(
            "parts = split(ref, \"-\")\nemit \"vx.t\", {p1: parts[1], j: join(parts, \"/\"), r: replace(ref, \"-\", \".\"), t: trim(pad), l: len(parts)}",
            json!({"ref": "FA-2026-77", "pad": "  x  "}),
        );
        assert_eq!(p["p1"], "2026");
        assert_eq!(p["j"], "FA/2026/77");
        assert_eq!(p["r"], "FA.2026.77");
        assert_eq!(p["t"], "x");
        assert_eq!(p["l"], 3);
    }

    #[test]
    fn unknown_function_and_parse_errors() {
        assert!(eval_flow("x = nope(1)\nemit \"vx.t\", {x: x}", json!({}))
            .unwrap_err()
            .contains("unknown function"));
        match parse("if x:\n  y = 1\n") {
            Err(e) => assert!(e.contains("missing `end`")),
            Ok(_) => panic!("expected parse error"),
        }
        assert!(parse("x = ").is_err());
    }

    #[test]
    fn emit_subject_via_constant_is_collected() {
        let prog = parse("Q = \"vx.out.q\"\nemit Q, {a: 1}").unwrap();
        assert_eq!(prog.emit_subjects, vec!["vx.out.q"]);
    }

    #[test]
    fn surface_extraction_kinds() {
        let prog =
            parse("T = {\"a\": \"b\"}\nN = 5\nL = [1, 2]\nS = \"x\"\nlower_ignored = 1\n").unwrap();
        let names: Vec<&str> = prog.surface.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["T", "N", "L", "S"]);
    }

    #[test]
    fn set_literal_constant_and_table_entry_and_list() {
        let src = "T = {\"haute\": \"P2\", \"basse\": \"P4\"}\nN = 250\nL = [\"P1\", \"P2\"]\nemit \"vx.t\", {n: N}\n";
        let s1 = set_literal(src, "N", "-", &json!(999)).unwrap();
        assert!(s1.contains("N = 999"));
        let s2 = set_literal(&s1, "T", "haute", &json!("P9")).unwrap();
        assert!(s2.contains("\"haute\": \"P9\""));
        assert!(s2.contains("\"basse\": \"P4\""));
        let s3 = set_literal(&s2, "L", "-", &json!(["P1"])).unwrap();
        assert!(s3.contains("L = [\"P1\"]"));
        // and the result still runs
        let emits = eval_flow(&s3, json!({})).unwrap();
        assert_eq!(emits[0].1["n"], 999);
        assert!(set_literal(src, "T", "absent", &json!("x")).is_err());
        assert!(set_literal(src, "NOPE", "-", &json!(1)).is_err());
    }

    #[test]
    fn invoke_capture_and_merge_and_exports() {
        let root = std::env::temp_dir().join(format!("vejas-test-{}", std::process::id()));
        let svc_default = root.join("services");
        let pkg = root.join("packages").join("billing");
        std::fs::create_dir_all(&svc_default).unwrap();
        std::fs::create_dir_all(pkg.join("services")).unwrap();
        std::fs::write(svc_default.join("double.vjs"), "twice = n * 2\n").unwrap();
        std::fs::write(pkg.join("services").join("fmt.vjs"), "label = f\"<{x}>\"\n").unwrap();
        std::fs::write(pkg.join("services").join("secret.vjs"), "y = 1\n").unwrap();
        std::fs::write(pkg.join("package.vjs"), "ENABLED = true\nEXPORTS = [\"fmt\"]\n").unwrap();

        let mut engine = Engine::new(root.clone(), "default".into());
        // merge form
        let prog = parse("invoke double(n: 21)\nemit \"vx.t\", {t: twice}").unwrap();
        let ctx = run(&prog, &json!({}), &mut engine).unwrap();
        assert_eq!(ctx.emits[0].1["t"], 42);
        // capture form + cross-package exported
        let prog2 = parse("d = invoke billing:fmt(x: \"ok\")\nemit \"vx.t\", {l: d.label}").unwrap();
        let ctx2 = run(&prog2, &json!({}), &mut engine).unwrap();
        assert_eq!(ctx2.emits[0].1["l"], "<ok>");
        // cross-package private -> denied
        let prog3 = parse("invoke billing:secret(a: 1)\n").unwrap();
        let err = match run(&prog3, &json!({}), &mut engine) {
            Err(e) => e,
            Ok(_) => panic!("expected exports denial"),
        };
        assert!(err.contains("not exported"), "{err}");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn service_emits_propagate_and_pkg_context_switches() {
        let root = std::env::temp_dir().join(format!("vejas-test2-{}", std::process::id()));
        let pkg = root.join("packages").join("n");
        std::fs::create_dir_all(pkg.join("services")).unwrap();
        std::fs::write(pkg.join("services").join("inner.vjs"), "r = a + 1\n").unwrap();
        std::fs::write(
            pkg.join("services").join("outer.vjs"),
            "invoke inner(a: v)\nemit \"vx.deep\", {r: r}\n",
        )
        .unwrap();
        std::fs::write(pkg.join("package.vjs"), "EXPORTS = [\"outer\"]\n").unwrap();
        let mut engine = Engine::new(root.clone(), "default".into());
        // outer is exported; inner is private but called FROM WITHIN pkg n -> allowed
        let prog = parse("invoke n:outer(v: 9)\n").unwrap();
        let ctx = run(&prog, &json!({}), &mut engine).unwrap();
        assert_eq!(ctx.emits, vec![("vx.deep".to_string(), json!({"r": 10}))]);
        let _ = std::fs::remove_dir_all(&root);
    }
}
