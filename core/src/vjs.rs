// VejasScript — embedded interpreter.
//
// The native flow language of Vejas, a small flow language
// interpreted in Rust, in-process.
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
//                                           pipeline into the caller (pipeline merge)
//   x = invoke svc(k: v, ...)               composition, captured as a document
//   expressions: literals, {doc: exprs}, [arrays], f"strings {expr}",
//     a.b.c paths, a?.b null-safe, x ?? y, == != < <= > >= in, and/or/not,
//     + - * /, doc[key], orders[].id projection, orders[status == "active"],
//     builtins: upper lower trim len str num split join replace round abs
//
// Pipeline model: the incoming event's top-level fields ARE
// the variable space (`event` also holds the whole document). An invoked
// service starts with exactly its named arguments as pipeline and returns
// its whole final pipeline.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use serde_json::{json, Map, Number, Value};

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
    Driver,
    Api,
    Respond,
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
    // a byte-order mark, which some editors write, is not code
    let from = if src.as_bytes().starts_with(&[0xEF, 0xBB, 0xBF]) { 3 } else { 0 };
    lex_from(src, from)
}

/// The tokens of `src` from `from` to its end, at their places in `src`. An
/// f-string's `{…}` is lexed in place, `src` cut at its closing brace, so that
/// an error in it, and the parser's after it, say the line of the file.
fn lex_from(src: &str, from: usize) -> Result<Vec<Spanned>, String> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = from;
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
                    "driver" => Tok::Driver,
                    "api" => Tok::Api,
                    "respond" => Tok::Respond,
                    "true" => Tok::True,
                    "false" => Tok::False,
                    "null" => Tok::Null,
                    _ => Tok::Ident(w.to_string()),
                };
                out.push(Spanned { tok, pos: start });
            }
            _ => {
                // `get`: a character of several bytes has no boundary at `i + 2`
                let two = src.get(i..i + 2).unwrap_or("");
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
                        _ => {
                            let found = src.get(i..).and_then(|rest| rest.chars().next()).unwrap_or(c);
                            return Err(err_at(i, src, &format!("unexpected character {found:?}")));
                        }
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

/// One escape in a string, the backslash at `at`: what it stands for goes to
/// `out`, and its length comes back. These are JSON's escapes, since a literal
/// set live is written back as JSON (`set_literal`), and `\{` `\}` for a brace
/// in an f-string. Any other is an error: it used to keep its letter alone, so
/// a CR set live came back as an `r`.
fn unescape(src: &str, at: usize, out: &mut Vec<u8>) -> Result<usize, String> {
    let b = src.as_bytes();
    let byte = match b.get(at + 1) {
        Some(b'n') => b'\n',
        Some(b't') => b'\t',
        Some(b'r') => b'\r',
        Some(b'b') => 0x08,
        Some(b'f') => 0x0c,
        Some(&c @ (b'"' | b'\\' | b'/' | b'{' | b'}')) => c,
        Some(b'u') => {
            let unit = |i: usize| {
                src.get(i..i + 4)
                    .filter(|h| h.bytes().all(|c| c.is_ascii_hexdigit()))
                    .and_then(|h| u32::from_str_radix(h, 16).ok())
            };
            let alone = || {
                err_at(at, src, "a \\u surrogate stands alone: a character above U+FFFF is written as a pair of \\u escapes")
            };
            let hi = unit(at + 2).ok_or_else(|| err_at(at, src, "\\u takes four hex digits"))?;
            let (code, len) = if (0xD800..0xDC00).contains(&hi) {
                let lo = (b.get(at + 6..at + 8) == Some(&b"\\u"[..]))
                    .then(|| unit(at + 8))
                    .flatten()
                    .filter(|lo| (0xDC00..0xE000).contains(lo))
                    .ok_or_else(alone)?;
                (0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00), 12)
            } else {
                (hi, 6)
            };
            let c = char::from_u32(code).ok_or_else(alone)?;
            out.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes());
            return Ok(len);
        }
        _ => {
            let c = src[at + 1..].chars().next().unwrap_or(' ');
            return Err(err_at(at, src, &format!("unknown escape \\{c} in a string: a backslash is written \\\\")));
        }
    };
    out.push(byte);
    Ok(2)
}

fn lex_string(src: &str, start: usize) -> Result<(String, usize), String> {
    let b = src.as_bytes();
    let mut s: Vec<u8> = Vec::new();
    let mut i = start + 1;
    while i < b.len() {
        match b[i] {
            b'"' => return Ok((String::from_utf8_lossy(&s).into_owned(), i + 1)),
            b'\\' if i + 1 < b.len() => i += unescape(src, i, &mut s)?,
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
                let inner = lex_from(&src[..j - 1], start)?;
                parts.push(FPart::Expr(inner));
                i = j;
            }
            b'\\' if i + 1 < b.len() => i += unescape(src, i, &mut lit)?,
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
    /// Each arm is (condition, condition source span, body). The span lets the
    /// rules-view (ADR-0019) show a non-projectable guard's exact source rather
    /// than a paraphrase; the interpreter ignores it.
    If(Vec<(Expr, (usize, usize), Block)>, Option<Block>),
    For(String, Expr, Block),
    Emit(Expr, Expr),
    /// `respond <status>, <body>` — set the synchronous HTTP response of an
    /// `api` flow (status code + JSON body). Side-effect `emit`s still go to the
    /// bus; `respond` is what the caller gets back.
    Respond(Expr, Expr),
    InvokeMerge(String, Vec<(String, Expr)>),
    /// `try:` … `catch e:` … `end` (ADR-0039, point 3): the statements, the
    /// name the catch gives the failure, and the catch's statements.
    Try(Block, Option<String>, Block),
    /// `remember <key> for <window>` (ADR-0039, point 4): the key, and the
    /// window, a duration the flow writes (`"7d"`) or a constant that holds one.
    Remember(Expr, Expr),
}

/// Statements in order, with the source line each one starts on: the trace
/// record says which statements an event went through (ADR-0034, point 8).
/// It reads as a slice of statements everywhere else.
#[derive(Clone, Debug, Default)]
pub struct Block {
    stmts: Vec<Stmt>,
    /// 0 for a statement the record leaves out: a business-surface constant,
    /// which every run assigns the same way. As long as `stmts`: only `push`
    /// adds to either.
    lines: Vec<u32>,
}

impl Block {
    fn push(&mut self, line: u32, stmt: Stmt) {
        self.stmts.push(stmt);
        self.lines.push(line);
    }
}

impl std::ops::Deref for Block {
    type Target = [Stmt];
    fn deref(&self) -> &[Stmt] {
        &self.stmts
    }
}

impl<'a> IntoIterator for &'a Block {
    type Item = &'a Stmt;
    type IntoIter = std::slice::Iter<'a, Stmt>;
    fn into_iter(self) -> Self::IntoIter {
        self.stmts.iter()
    }
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
    /// When set, this flow is exposed as a synchronous HTTP endpoint:
    /// `api "POST /orders"` / `api "GET /orders/{id}"`. The runtime routes
    /// (method, path) here, runs it on the body as sent with its {path params}
    /// as variables (ADR-0039), and returns the flow's `respond`. Several flows
    /// (one per verb) compose a REST resource.
    pub api: Option<String>,
    /// When set, this file is a connector instance manifest: `driver "http-in"`
    /// plus UPPERCASE literal config. It is run by a native driver, not as a flow.
    pub driver: Option<String>,
    pub stmts: Block,
    pub surface: Vec<SurfaceEntry>,
    pub emit_subjects: Vec<String>,
    pub invokes: Vec<String>,
    /// Top-level `NAME = secret("path/key")` references (name, reference).
    /// Lets the panel show which secrets a flow/connector uses, never values.
    pub secret_refs: Vec<(String, String)>,
}

// ───────────────────────── rules-view projection (ADR-0019) ─────────────────────────
//
// N2 of the rule-editing doctrine: a READ-ONLY projection of a flow's top-level
// `if/elif/else` into sentences a domain expert can read, with the surface
// literals they reference marked inline-editable (they reuse the existing
// Apply→replay→Promote loop — no new write path). The projection is FAITHFUL or
// RAW, never a lossy paraphrase and never half-French/half-code: an arm whose
// guard is EXACTLY renderable (one comparison of simple operands) becomes a
// sentence; anything composed (and/or, optional `?.`, projections, calls) is
// shown as its verbatim source, flagged "advanced", and pointed at the agent
// loop (N3). Granularity is per-arm and binary.

/// Render an operand of a comparison, faithfully, or None if it is not a simple
/// value navigation (which forces the whole guard to RAW).
fn render_atom(e: &Expr) -> Option<String> {
    match e {
        Expr::Var(n) => Some(n.clone()),
        Expr::Lit(Value::String(s)) => Some(format!("\"{s}\"")),
        Expr::Lit(v) => Some(v.to_string()),
        // plain field access only: an optional `?.` (flag = true) is "advanced".
        Expr::Field(b, name, false) => Some(format!("{}.{name}", render_atom(b)?)),
        Expr::Index(b, idx) => Some(format!("{}[{}]", render_atom(b)?, render_atom(idx)?)),
        _ => None,
    }
}

/// Collect the surface-constant names an operand references (UPPERCASE Vars that
/// exist in the flow's surface) — those become the inline-editable literals.
fn collect_surface_refs(e: &Expr, surface: &[String], out: &mut Vec<String>) {
    match e {
        Expr::Var(n) if surface.iter().any(|s| s == n) => {
            if !out.contains(n) {
                out.push(n.clone());
            }
        }
        Expr::Field(b, _, _) => collect_surface_refs(b, surface, out),
        Expr::Index(b, i, ..) => {
            collect_surface_refs(b, surface, out);
            collect_surface_refs(i, surface, out);
        }
        _ => {}
    }
}

fn cmp_symbol(op: BinOp) -> Option<&'static str> {
    match op {
        BinOp::Eq => Some("="),
        BinOp::Ne => Some("≠"),
        BinOp::Lt => Some("<"),
        BinOp::Le => Some("≤"),
        BinOp::Gt => Some(">"),
        BinOp::Ge => Some("≥"),
        BinOp::In => Some("in"),
        _ => None, // and/or/coalesce/arithmetic are not a single comparison
    }
}

/// A projectable guard is exactly ONE comparison of two simple operands. Returns
/// the rendered sentence fragment and the surface literals it references.
fn render_guard(cond: &Expr, surface: &[String]) -> Option<(String, Vec<String>)> {
    if let Expr::Bin(op, a, b) = cond {
        let sym = cmp_symbol(*op)?;
        let (la, lb) = (render_atom(a)?, render_atom(b)?);
        let mut refs = Vec::new();
        collect_surface_refs(a, surface, &mut refs);
        collect_surface_refs(b, surface, &mut refs);
        return Some((format!("{la} {sym} {lb}"), refs));
    }
    None
}

/// Resolve an emit subject to a concrete string IFF it is statically known: a
/// string literal, or a Var naming a surface string constant (already in the
/// business surface, so resolvable without loss). None otherwise (dynamic).
fn resolve_subject(subject: &Expr, surface: &[SurfaceEntry]) -> Option<String> {
    match subject {
        Expr::Lit(Value::String(s)) => Some(s.clone()),
        Expr::Var(name) => surface
            .iter()
            .find(|e| &e.name == name)
            .and_then(|e| e.value.as_str().map(|s| s.to_string())),
        _ => None,
    }
}

/// The action a projectable arm performs: its emit subjects (and `respond`),
/// rendered only when each subject is statically resolvable (a literal or a
/// surface string constant) and the body has no nested control flow. Pure
/// assignments before the emit are internal plumbing and allowed. None means the
/// body is not a simple action → the arm is RAW.
fn render_action(body: &[Stmt], surface: &[SurfaceEntry]) -> Option<Vec<String>> {
    let mut acts = Vec::new();
    for st in body {
        match st {
            Stmt::Emit(subject, _) => match resolve_subject(subject, surface) {
                Some(s) => acts.push(format!("→ {s}")),
                None => return None, // dynamic subject: not exactly renderable
            },
            Stmt::Respond(status, _) => acts.push(match status {
                Expr::Lit(v) => format!("respond {v}"),
                _ => "respond".to_string(),
            }),
            // assignments are internal plumbing — allowed, not shown
            Stmt::Assign(_, _) | Stmt::InvokeMerge(_, _) | Stmt::Remember(..) => {}
            // nested control flow makes the arm advanced → RAW
            Stmt::If(..) | Stmt::For(..) | Stmt::Try(..) => return None,
        }
    }
    if acts.is_empty() {
        None // no visible action to project
    } else {
        Some(acts)
    }
}

/// Project a flow's top-level `if/elif/else` arms into rule views (ADR-0019).
/// `src` is the flow source (for the verbatim RAW slice); `surface` is the flow's
/// business-surface entries — their names drive the inline-editable condition
/// literals, and their string values let an emit subject held in a constant
/// (`emit OUT, …`) resolve statically instead of falling to RAW.
pub fn flow_rules(prog: &Program, src: &str, surface: &[SurfaceEntry]) -> Vec<Value> {
    let names: Vec<String> = surface.iter().map(|e| e.name.clone()).collect();
    let mut rules = Vec::new();
    for st in &prog.stmts {
        let Stmt::If(arms, otherwise) = st else { continue };
        for (i, (cond, span, body)) in arms.iter().enumerate() {
            let kind = if i == 0 { "if" } else { "elif" };
            let raw = src.get(span.0..span.1).map(|s| s.trim()).unwrap_or("");
            match (render_guard(cond, &names), render_action(body, surface)) {
                (Some((when, literals)), Some(then)) => rules.push(json!({
                    "kind": kind, "projectable": true,
                    "when": when, "then": then, "literals": literals,
                })),
                _ => rules.push(json!({
                    "kind": kind, "projectable": false, "raw": raw,
                })),
            }
        }
        if let Some(body) = otherwise {
            match render_action(body, surface) {
                Some(then) => rules.push(json!({
                    "kind": "else", "projectable": true,
                    "when": Value::Null, "then": then, "literals": [],
                })),
                None => rules.push(json!({
                    "kind": "else", "projectable": false, "raw": "…",
                })),
            }
        }
    }
    rules
}

// ───────────────────────── flow shape (the panel's flow page) ─────────────────────────
//
// The rules-view widened to the whole flow: what it reads from the event, how
// each value it computes is made, where it sends. Read-only like the
// rules-view: the panel changes a flow's literals through the surface, never
// through this.

/// The comment block just above a literal: what its author wrote for whoever
/// changes it ("Only payments strictly above this amount raise an alert"). The
/// panel shows it as the literal's label. `value_start` is where the literal's
/// value starts in `src`; a blank line or a file header (`# flow: name`) ends
/// the block.
pub fn literal_doc(src: &str, value_start: usize) -> Option<String> {
    let before = src.get(..value_start)?;
    let mut end = before.rfind('\n')?; // the newline that ends the line above
    let mut docs = Vec::new();
    loop {
        let start = src[..end].rfind('\n').map(|i| i + 1).unwrap_or(0);
        let Some(text) = src[start..end].trim().strip_prefix('#') else { break };
        let text = text.trim();
        let header = ["flow:", "service:", "connector:", "package:"].iter().any(|h| text.starts_with(h));
        if header {
            break;
        }
        docs.push(text);
        if start == 0 {
            break;
        }
        end = start - 1;
    }
    docs.reverse();
    let doc = docs.join(" ");
    (!doc.trim().is_empty()).then_some(doc)
}

/// What a flow reads from its event, in the order the code first reads it:
/// the bare names it never assigns (`id`, `email`), and the paths under them
/// or under `event` (`shipping_address.country`, `event.total` as `total`).
fn collect_reads(e: &Expr, own: &[String], out: &mut Vec<String>) {
    reads_of(e, own, false, out)
}

/// `own` holds names (`total`) and the paths the flow set inside one
/// (`customer.email`): a path under one of them is the flow's. `item_keys`:
/// inside a filter's condition, `line_items[qty > 10]`, where a bare name is
/// a key of each item, which only the run knows; there only the `event.`
/// paths and the business literals are the flow's.
fn reads_of(e: &Expr, own: &[String], item_keys: bool, out: &mut Vec<String>) {
    let mut push = |name: String| {
        if !out.contains(&name) {
            out.push(name);
        }
    };
    let the_flows = |root: &str| !item_keys || is_upper_name(root);
    match e {
        Expr::Var(n) if n != "event" && !own.contains(n) && the_flows(n) => push(n.clone()),
        Expr::Field(..) => {
            // the dotted path, when it bottoms out at a name
            let mut segments = Vec::new();
            let mut at = e;
            while let Expr::Field(base, name, _) = at {
                segments.push(name.clone());
                at = base;
            }
            segments.reverse();
            let set_by_the_flow = |root: &str| {
                let mut path = root.to_string();
                own.contains(&path)
                    || segments.iter().any(|s| {
                        path.push('.');
                        path.push_str(s);
                        own.contains(&path)
                    })
            };
            match at {
                Expr::Var(root) if root == "event" => {
                    if !segments.is_empty() {
                        push(segments.join("."));
                    }
                }
                Expr::Var(root) if the_flows(root) && !set_by_the_flow(root) => {
                    segments.insert(0, root.clone());
                    push(segments.join("."));
                }
                // a path the flow set, or an item's key: not the event's
                Expr::Var(_) => {}
                other => reads_of(other, own, item_keys, out),
            }
        }
        Expr::Var(_) | Expr::Lit(_) => {}
        Expr::FString(parts) => {
            for p in parts {
                if let FStringPart::Expr(e) = p {
                    reads_of(e, own, item_keys, out);
                }
            }
        }
        Expr::Doc(fields) => fields.iter().for_each(|(_, v)| reads_of(v, own, item_keys, out)),
        Expr::Array(items) => items.iter().for_each(|i| reads_of(i, own, item_keys, out)),
        Expr::Projection(b) | Expr::Unary(_, b) => reads_of(b, own, item_keys, out),
        Expr::Index(a, b) | Expr::Bin(_, a, b) => {
            reads_of(a, own, item_keys, out);
            reads_of(b, own, item_keys, out);
        }
        Expr::Filter(items, cond) => {
            reads_of(items, own, item_keys, out);
            reads_of(cond, own, true, out);
        }
        Expr::Call(_, args) => args.iter().for_each(|a| reads_of(a, own, item_keys, out)),
        Expr::Invoke(_, args) => args.iter().for_each(|(_, v)| reads_of(v, own, item_keys, out)),
    }
}

/// Every name a flow gives a value to, anywhere: its assignments, its loop
/// variables, its business literals.
fn collect_own(stmts: &[Stmt], out: &mut Vec<String>) {
    for s in stmts {
        match s {
            Stmt::Assign(path, _) => {
                if let Some(root) = path.first() {
                    if !out.contains(root) {
                        out.push(root.clone());
                    }
                }
            }
            Stmt::For(var, _, body) => {
                if !out.contains(var) {
                    out.push(var.clone());
                }
                collect_own(body, out);
            }
            Stmt::If(arms, otherwise) => {
                arms.iter().for_each(|(_, _, body)| collect_own(body, out));
                if let Some(body) = otherwise {
                    collect_own(body, out);
                }
            }
            Stmt::Try(body, name, handler) => {
                collect_own(body, out);
                if let Some(name) = name {
                    if !out.contains(name) {
                        out.push(name.clone());
                    }
                }
                collect_own(handler, out);
            }
            Stmt::Emit(..) | Stmt::Respond(..) | Stmt::InvokeMerge(..) | Stmt::Remember(..) => {}
        }
    }
}

/// How many statements give `name` a value, at any depth: an assignment, a
/// loop's variable, a catch's name.
fn assignments(block: &Block, name: &str) -> usize {
    block
        .stmts
        .iter()
        .map(|st| match st {
            Stmt::Assign(path, _) => usize::from(path.first().is_some_and(|first| first == name)),
            Stmt::For(var, _, body) => usize::from(var == name) + assignments(body, name),
            Stmt::If(arms, otherwise) => {
                arms.iter().map(|(_, _, body)| assignments(body, name)).sum::<usize>()
                    + otherwise.as_ref().map_or(0, |body| assignments(body, name))
            }
            Stmt::Try(body, caught, handler) => {
                assignments(body, name) + usize::from(caught.as_deref() == Some(name)) + assignments(handler, name)
            }
            Stmt::Emit(..) | Stmt::Respond(..) | Stmt::InvokeMerge(..) | Stmt::Remember(..) => 0,
        })
        .sum()
}

/// Each `remember` whose window names a constant gets the constant's literal
/// (ADR-0039): the parser checked it holds a duration.
fn windows_from_constants(block: &mut Block, constants: &HashMap<&str, &Value>) {
    for st in block.stmts.iter_mut() {
        match st {
            Stmt::Remember(_, window) => {
                if let Expr::Var(name) = window {
                    if let Some(value) = constants.get(name.as_str()) {
                        *window = Expr::Lit((*value).clone());
                    }
                }
            }
            Stmt::If(arms, otherwise) => {
                for (_, _, body) in arms.iter_mut() {
                    windows_from_constants(body, constants);
                }
                if let Some(body) = otherwise {
                    windows_from_constants(body, constants);
                }
            }
            Stmt::For(_, _, body) => windows_from_constants(body, constants),
            Stmt::Try(body, _, handler) => {
                windows_from_constants(body, constants);
                windows_from_constants(handler, constants);
            }
            Stmt::Assign(..) | Stmt::Emit(..) | Stmt::Respond(..) | Stmt::InvokeMerge(..) => {}
        }
    }
}

/// A name in capitals (`MIN_TOTAL_EUR`): how a business literal is named.
fn is_upper_name(name: &str) -> bool {
    name.chars().all(|c| c.is_ascii_uppercase() || c == '_' || c.is_ascii_digit())
        && name.chars().any(|c| c.is_ascii_uppercase())
}

/// What a flow reads from its event, in the order it runs: a name read before
/// the flow gives it a value (`email = lower(email)` reads the event's email).
/// A branch or a loop body starts from what was set before it, and what it
/// sets counts after it. An invoke gives back its arguments and what the
/// service sets (`makes`, keyed by the name the invoke uses).
/// The top-level fields of its event a flow may read, or `event` alone when
/// it reads the event whole (`event[KEY]`, `emit "x", event`): what a field
/// hidden from a shadow run can change (ADR-0034). Every name the code reads
/// counts, but its business literals, even one a branch sets first: the
/// other branch leaves it to the event. That errs on the side of naming a
/// field.
pub fn event_reads(prog: &Program) -> Vec<String> {
    if stmts_read_whole_event(&prog.stmts) {
        return vec!["event".to_string()];
    }
    let own: Vec<String> = prog.surface.iter().map(|e| e.name.clone()).collect();
    let mut reads = Vec::new();
    reads_of_stmts(&prog.stmts, &own, &mut reads);
    let mut top: Vec<String> = Vec::new();
    for r in reads {
        let name = r.split('.').next().unwrap_or(&r).to_string();
        if !top.contains(&name) {
            top.push(name);
        }
    }
    top
}

fn stmts_read_whole_event(stmts: &[Stmt]) -> bool {
    stmts.iter().any(|s| match s {
        Stmt::Assign(_, e) => reads_whole_event(e),
        Stmt::For(_, iter, body) => reads_whole_event(iter) || stmts_read_whole_event(body),
        Stmt::If(arms, otherwise) => {
            arms.iter().any(|(cond, _, body)| reads_whole_event(cond) || stmts_read_whole_event(body))
                || otherwise.as_ref().is_some_and(|body| stmts_read_whole_event(body))
        }
        Stmt::Emit(a, b) | Stmt::Respond(a, b) | Stmt::Remember(a, b) => reads_whole_event(a) || reads_whole_event(b),
        Stmt::InvokeMerge(_, args) => args.iter().any(|(_, v)| reads_whole_event(v)),
        Stmt::Try(body, _, handler) => stmts_read_whole_event(body) || stmts_read_whole_event(handler),
    })
}

/// Does `e` read the event whole? `event.total` reads one field of it.
fn reads_whole_event(e: &Expr) -> bool {
    match e {
        Expr::Var(name) => name == "event",
        Expr::Lit(_) => false,
        Expr::FString(parts) => parts.iter().any(|p| matches!(p, FStringPart::Expr(e) if reads_whole_event(e))),
        Expr::Doc(pairs) | Expr::Invoke(_, pairs) => pairs.iter().any(|(_, e)| reads_whole_event(e)),
        Expr::Array(es) | Expr::Call(_, es) => es.iter().any(reads_whole_event),
        Expr::Field(base, _, _) => !matches!(**base, Expr::Var(_)) && reads_whole_event(base),
        Expr::Index(a, b) | Expr::Filter(a, b) | Expr::Bin(_, a, b) => reads_whole_event(a) || reads_whole_event(b),
        Expr::Projection(b) | Expr::Unary(_, b) => reads_whole_event(b),
    }
}

fn reads_in_order(stmts: &[Stmt], set: &mut Vec<String>, makes: &[(String, Vec<String>)], out: &mut Vec<String>) {
    fn add(set: &mut Vec<String>, name: &str) {
        if !set.iter().any(|n| n == name) {
            set.push(name.to_string());
        }
    }
    for s in stmts {
        match s {
            Stmt::Assign(path, e) => {
                collect_reads(e, set, out);
                // `customer.email = …` sets that field: the event's other
                // fields under `customer` are still the event's
                if !path.is_empty() {
                    add(set, &path.join("."));
                }
            }
            Stmt::For(var, iter, body) => {
                collect_reads(iter, set, out);
                let mut inner = set.clone();
                add(&mut inner, var);
                reads_in_order(body, &mut inner, makes, out);
                inner.iter().for_each(|n| add(set, n));
            }
            Stmt::If(arms, otherwise) => {
                let before = set.clone();
                let mut walk = |body: &[Stmt], out: &mut Vec<String>| {
                    let mut inner = before.clone();
                    reads_in_order(body, &mut inner, makes, out);
                    inner.iter().for_each(|n| add(set, n));
                };
                for (cond, _, body) in arms {
                    collect_reads(cond, &before, out);
                    walk(body, out);
                }
                if let Some(body) = otherwise {
                    walk(body, out);
                }
            }
            Stmt::Emit(a, b) | Stmt::Respond(a, b) | Stmt::Remember(a, b) => {
                collect_reads(a, set, out);
                collect_reads(b, set, out);
            }
            // the catch starts from what was set before the try, and its
            // failure; what either sets counts after it
            Stmt::Try(body, name, handler) => {
                let before = set.clone();
                let mut inner = before.clone();
                reads_in_order(body, &mut inner, makes, out);
                inner.iter().for_each(|n| add(set, n));
                let mut inner = before;
                if let Some(name) = name {
                    add(&mut inner, name);
                }
                reads_in_order(handler, &mut inner, makes, out);
                inner.iter().for_each(|n| add(set, n));
            }
            Stmt::InvokeMerge(service, args) => {
                args.iter().for_each(|(_, v)| collect_reads(v, set, out));
                args.iter().for_each(|(name, _)| add(set, name));
                for (_, names) in makes.iter().filter(|(s, _)| s == service) {
                    names.iter().for_each(|n| add(set, n));
                }
            }
        }
    }
}

fn reads_of_stmts(stmts: &[Stmt], own: &[String], out: &mut Vec<String>) {
    for s in stmts {
        match s {
            Stmt::Assign(_, e) => collect_reads(e, own, out),
            Stmt::For(_, iter, body) => {
                collect_reads(iter, own, out);
                reads_of_stmts(body, own, out);
            }
            Stmt::If(arms, otherwise) => {
                for (cond, _, body) in arms {
                    collect_reads(cond, own, out);
                    reads_of_stmts(body, own, out);
                }
                if let Some(body) = otherwise {
                    reads_of_stmts(body, own, out);
                }
            }
            Stmt::Emit(a, b) | Stmt::Respond(a, b) | Stmt::Remember(a, b) => {
                collect_reads(a, own, out);
                collect_reads(b, own, out);
            }
            Stmt::InvokeMerge(_, args) => args.iter().for_each(|(_, v)| collect_reads(v, own, out)),
            Stmt::Try(body, name, handler) => {
                reads_of_stmts(body, own, out);
                let mut own = own.to_vec();
                own.extend(name.iter().cloned());
                reads_of_stmts(handler, &own, out);
            }
        }
    }
}

/// The emits under `stmts`, wherever they sit, with the fields they send.
fn collect_sends(stmts: &[Stmt], surface: &[SurfaceEntry], inside: Option<&str>, out: &mut Vec<Value>) {
    for s in stmts {
        match s {
            Stmt::Emit(subject, body) => {
                let constant = match subject {
                    Expr::Var(n) if surface.iter().any(|e| &e.name == n) => Some(n.clone()),
                    _ => None,
                };
                let fields: Vec<Value> = match body {
                    Expr::Doc(pairs) => pairs
                        .iter()
                        .map(|(name, v)| json!({"name": name, "from": render_atom(v)}))
                        .collect(),
                    _ => Vec::new(),
                };
                out.push(json!({
                    "subject": resolve_subject(subject, surface),
                    "constant": constant,
                    "fields": fields,
                    "whole": match body { Expr::Doc(_) => None, other => render_atom(other) },
                    "inside": inside,
                }));
            }
            Stmt::If(arms, otherwise) => {
                arms.iter().for_each(|(_, _, body)| collect_sends(body, surface, Some("when"), out));
                if let Some(body) = otherwise {
                    collect_sends(body, surface, Some("when"), out);
                }
            }
            Stmt::For(_, _, body) => collect_sends(body, surface, Some("each"), out),
            // what a catch sends goes out on a failure: a condition too
            Stmt::Try(body, _, handler) => {
                collect_sends(body, surface, inside, out);
                collect_sends(handler, surface, Some("when"), out);
            }
            Stmt::Assign(..) | Stmt::Respond(..) | Stmt::InvokeMerge(..) | Stmt::Remember(..) => {}
        }
    }
}

/// The source of top-level statement `i`: from its line to the line before the
/// next statement's, without the blank and comment lines that trail it.
fn statement_text(src_lines: &[&str], starts: &[u32], i: usize) -> String {
    let Some(&first) = starts.get(i) else { return String::new() };
    if first == 0 {
        return String::new();
    }
    let next = starts[i + 1..].iter().copied().find(|&l| l > first).map(|l| l as usize - 1);
    let last = next.unwrap_or(src_lines.len()).min(src_lines.len());
    let mut lines: Vec<&str> = src_lines[first as usize - 1..last].to_vec();
    while lines.last().is_some_and(|l| l.trim().is_empty() || l.trim_start().starts_with('#')) {
        lines.pop();
    }
    lines.join("\n")
}

/// One flow as its page shows it: the event fields it reads, its top-level
/// statements in order with their source text, and where it sends. A `set`
/// step is a value the flow computes (`order_ref = split(id, "#")[1]`); `each`
/// a loop and what it builds; `when` a condition, which the rules-view
/// renders; `invoke` a service it composes. A business literal is no step: the
/// surface carries it. `services` holds what each service the flow invokes
/// gives a value to: those values come from the service, not the event.
pub fn flow_shape(prog: &Program, src: &str, services: &[(String, Vec<String>)]) -> Value {
    let surface: Vec<String> = prog.surface.iter().map(|e| e.name.clone()).collect();
    let mut own = surface.clone();
    collect_own(&prog.stmts, &mut own);
    let mut made_by = serde_json::Map::new();
    for (service, names) in services {
        for name in names {
            if !own.contains(name) {
                own.push(name.clone());
                made_by.entry(name.clone()).or_insert_with(|| json!(service));
            }
        }
    }
    let mut reads = Vec::new();
    reads_in_order(&prog.stmts, &mut surface.clone(), services, &mut reads);

    // A value a loop builds (`positions = []`, then `positions = positions + …`
    // in the loop) is one step, the loop's: its empty start is no step.
    let mut built: Vec<String> = Vec::new();
    for s in prog.stmts.iter() {
        if let Stmt::For(_, _, body) = s {
            collect_own(body, &mut built);
        }
    }

    let src_lines: Vec<&str> = src.lines().collect();
    let starts = &prog.stmts.lines;
    let mut steps = Vec::new();
    for (i, s) in prog.stmts.iter().enumerate() {
        let line = starts[i];
        if line == 0 {
            continue; // a business literal
        }
        let text = statement_text(&src_lines, starts, i);
        // every name a statement reads (event fields, values made before,
        // literals), the literals apart: the page edits them in place
        let split = |mut used: Vec<String>| {
            let mut literals = Vec::new();
            used.retain(|n| {
                let root = n.split('.').next().unwrap_or(n);
                if surface.iter().any(|s| s == root) {
                    if !literals.contains(&root.to_string()) {
                        literals.push(root.to_string());
                    }
                    false
                } else {
                    true
                }
            });
            (used, literals)
        };
        let uses = |e: &Expr| {
            let mut used = Vec::new();
            collect_reads(e, &[], &mut used);
            split(used)
        };
        match s {
            Stmt::Assign(path, e) => {
                let name = path.join(".");
                let empty_start = matches!(e, Expr::Array(a) if a.is_empty())
                    || matches!(e, Expr::Doc(d) if d.is_empty())
                    || matches!(e, Expr::Lit(_));
                if path.len() == 1 && built.contains(&path[0]) && empty_start {
                    continue;
                }
                let code = text.split_once('=').map(|(_, rhs)| rhs.trim()).unwrap_or(&text).to_string();
                let (used, literals) = uses(e);
                steps.push(json!({"kind": "set", "line": line, "name": name, "code": code,
                                  "uses": used, "literals": literals}));
            }
            Stmt::For(var, iter, body) => {
                let mut builds = Vec::new();
                collect_own(body, &mut builds);
                builds.retain(|n| n != var);
                let mut read = Vec::new();
                collect_reads(iter, &[], &mut read);
                reads_of_stmts(body, std::slice::from_ref(var), &mut read);
                let (used, literals) = split(read);
                steps.push(json!({"kind": "each", "line": line, "var": var, "over": render_atom(iter),
                                  "builds": builds, "code": text, "uses": used, "literals": literals}));
            }
            // a try runs its catch on a failure: a condition, as the page shows one
            Stmt::If(..) | Stmt::Try(..) => steps.push(json!({"kind": "when", "line": line, "code": text})),
            Stmt::Remember(key, window) => {
                let (mut used, mut literals) = uses(key);
                let (more, more_literals) = uses(window);
                used.extend(more.into_iter().filter(|n| !used.contains(n)).collect::<Vec<_>>());
                literals.extend(more_literals.into_iter().filter(|n| !literals.contains(n)).collect::<Vec<_>>());
                steps.push(json!({"kind": "remember", "line": line, "code": text, "uses": used, "literals": literals}));
            }
            Stmt::Emit(a, b) | Stmt::Respond(a, b) => {
                let kind = if matches!(s, Stmt::Emit(..)) { "emit" } else { "respond" };
                let (mut used, mut literals) = uses(a);
                let (more, more_literals) = uses(b);
                used.extend(more.into_iter().filter(|n| !used.contains(n)).collect::<Vec<_>>());
                literals.extend(more_literals.into_iter().filter(|n| !literals.contains(n)).collect::<Vec<_>>());
                steps.push(json!({"kind": kind, "line": line, "code": text, "uses": used, "literals": literals}));
            }
            Stmt::InvokeMerge(service, args) => {
                let (mut used, mut literals) = (Vec::new(), Vec::new());
                for (_, v) in args {
                    let (u, l) = uses(v);
                    used.extend(u.into_iter().filter(|n| !used.contains(n)).collect::<Vec<_>>());
                    literals.extend(l.into_iter().filter(|n| !literals.contains(n)).collect::<Vec<_>>());
                }
                steps.push(json!({"kind": "invoke", "line": line, "service": service, "code": text,
                                  "uses": used, "literals": literals}))
            }
        }
    }
    let mut sends = Vec::new();
    collect_sends(&prog.stmts, &prog.surface, None, &mut sends);
    json!({
        "reads": reads,
        "steps": steps,
        "sends": sends,
        "invokes": prog.invokes,
        "made_by": made_by,
        "docs": prog.surface.iter()
            .filter_map(|e| literal_doc(src, e.value_span.0).map(|d| (e.name.clone(), Value::String(d))))
            .collect::<serde_json::Map<String, Value>>(),
    })
}

// ───────────────────────────── parser ─────────────────────────────

struct Parser<'a> {
    toks: &'a [Spanned],
    i: usize,
    src: &'a str,
    /// Where each line ends in `src`, to give a statement its line.
    newlines: &'a [usize],
    /// Each `remember` window a constant holds, with where it is named: the
    /// constant is checked once the file is read, wherever it is written.
    windows: Vec<(String, usize)>,
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
    /// The line the next token is on, from 1.
    fn line(&self) -> u32 {
        let pos = self.pos();
        (self.newlines.partition_point(|&nl| nl < pos) + 1) as u32
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
    /// A name in key position: doc keys, field names after `.`, assignment
    /// path segments, invoke argument names. Keywords are allowed HERE (an
    /// EvidenceFact has a field literally called `source`; real-world JSON has
    /// keys named `in`, `end`, `tool`…) — but never as variable names.
    fn word(&mut self, what: &str) -> Result<String, String> {
        let w = match self.peek() {
            Tok::Ident(s) | Tok::Str(s) => Some(s.clone()),
            Tok::If => Some("if".into()),
            Tok::Elif => Some("elif".into()),
            Tok::Else => Some("else".into()),
            Tok::End => Some("end".into()),
            Tok::For => Some("for".into()),
            Tok::In => Some("in".into()),
            Tok::And => Some("and".into()),
            Tok::Or => Some("or".into()),
            Tok::Not => Some("not".into()),
            Tok::Emit => Some("emit".into()),
            Tok::Invoke => Some("invoke".into()),
            Tok::Source => Some("source".into()),
            Tok::Tool => Some("tool".into()),
            Tok::Driver => Some("driver".into()),
            Tok::Api => Some("api".into()),
            Tok::Respond => Some("respond".into()),
            Tok::True => Some("true".into()),
            Tok::False => Some("false".into()),
            Tok::Null => Some("null".into()),
            _ => None,
        };
        match w {
            Some(w) => {
                self.i += 1;
                Ok(w)
            }
            None => Err(self.err(&format!("expected {what}, found {:?}", self.peek()))),
        }
    }
    fn at_end(&self) -> bool {
        self.i >= self.toks.len() - 1
    }
    /// Whether a `catch` starts here: `catch:` or `catch e:`. `try`, `catch`
    /// and `remember` are words of the language only where they start their
    /// statement, so that a variable or a field of an event keeps such a name.
    fn at_catch(&self) -> bool {
        matches!(self.peek(), Tok::Ident(w) if w == "catch")
            && match self.peek2() {
                Some(Tok::Colon) => true,
                Some(Tok::Ident(_)) => matches!(self.toks.get(self.i + 2).map(|s| &s.tok), Some(Tok::Colon)),
                _ => false,
            }
    }

    fn program(&mut self) -> Result<Program, String> {
        let mut source = None;
        let mut tool = None;
        let mut driver = None;
        let mut api = None;
        let mut stmts = Block::default();
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
            if *self.peek() == Tok::Driver {
                self.next();
                match self.next() {
                    Tok::Str(s) => driver = Some(s),
                    _ => return Err(self.err("driver expects a string driver name")),
                }
                continue;
            }
            if *self.peek() == Tok::Api {
                self.next();
                match self.next() {
                    Tok::Str(s) => api = Some(s),
                    _ => return Err(self.err("api expects a string like \"POST /orders\"")),
                }
                continue;
            }
            if let Tok::Ident(name) = self.peek().clone() {
                if is_upper_name(&name) && self.peek2() == Some(&Tok::Eq) {
                    let save = self.i;
                    self.i += 2;
                    let start = self.pos();
                    if let Some((value, entry_spans, end)) = self.try_literal()? {
                        stmts.push(0, Stmt::Assign(vec![name.clone()], Expr::Lit(value.clone())));
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
            let line = self.line();
            let stmt = self.stmt()?;
            stmts.push(line, stmt);
        }
        for (name, at) in &self.windows {
            let held = surface.iter().find(|e| &e.name == name).map(|e| &e.value);
            match held {
                Some(Value::String(text)) => {
                    window_of(text).map_err(|e| err_at(*at, self.src, &format!("{name}: {e}")))?;
                    // the window is the constant's literal: another value
                    // the flow gives it would not be the window
                    if assignments(&stmts, name) > 1 {
                        return Err(err_at(*at, self.src, &format!("the window of `remember` is the literal of {name}, and the flow sets {name} elsewhere too: give that window a constant of its own")));
                    }
                }
                _ => {
                    return Err(err_at(*at, self.src, &format!("the window of `remember` is a duration: {name} is no constant of the flow that holds one, as {name} = \"7d\" does")))
                }
            }
        }
        // A window a constant holds is the constant's literal: read at run
        // time, the variable could be an event's field of the same name,
        // since the constant is assigned where it is written.
        let windows: HashMap<&str, &Value> = self
            .windows
            .iter()
            .filter_map(|(name, _)| surface.iter().find(|e| &e.name == name).map(|e| (name.as_str(), &e.value)))
            .collect();
        if !windows.is_empty() {
            windows_from_constants(&mut stmts, &windows);
        }
        let mut prog = Program {
            source,
            tool,
            driver,
            api,
            stmts,
            surface,
            emit_subjects: Vec::new(),
            invokes: Vec::new(),
            secret_refs: Vec::new(),
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
        // static secret references: EVERY secret("path/key") call, wherever it
        // sits — a direct assignment, a HEADERS doc, an f-string, a condition.
        // The panel's Secrets inventory depends on this being exhaustive.
        fn walk_expr(e: &Expr, ctx: &str, out: &mut Vec<(String, String)>) {
            match e {
                Expr::Call(name, args) => {
                    if name == "secret" {
                        if let Some(Expr::Lit(Value::String(r))) = args.first() {
                            let entry = (ctx.to_string(), r.clone());
                            if !out.contains(&entry) {
                                out.push(entry);
                            }
                        }
                    }
                    for a in args {
                        walk_expr(a, ctx, out);
                    }
                }
                Expr::FString(parts) => {
                    for p in parts {
                        if let FStringPart::Expr(e) = p {
                            walk_expr(e, ctx, out);
                        }
                    }
                }
                Expr::Doc(fields) => {
                    for (_, v) in fields {
                        walk_expr(v, ctx, out);
                    }
                }
                Expr::Array(items) => {
                    for i in items {
                        walk_expr(i, ctx, out);
                    }
                }
                Expr::Field(b, _, _) | Expr::Projection(b) | Expr::Unary(_, b) => {
                    walk_expr(b, ctx, out)
                }
                Expr::Index(a, b) | Expr::Filter(a, b) | Expr::Bin(_, a, b) => {
                    walk_expr(a, ctx, out);
                    walk_expr(b, ctx, out);
                }
                Expr::Invoke(_, args) => {
                    for (_, v) in args {
                        walk_expr(v, ctx, out);
                    }
                }
                Expr::Lit(_) | Expr::Var(_) => {}
            }
        }
        fn walk_stmts(stmts: &[Stmt], out: &mut Vec<(String, String)>) {
            for s in stmts {
                match s {
                    Stmt::Assign(path, e) => walk_expr(e, &path.join("."), out),
                    Stmt::Emit(a, b) => {
                        walk_expr(a, "emit", out);
                        walk_expr(b, "emit", out);
                    }
                    Stmt::Respond(a, b) => {
                        walk_expr(a, "respond", out);
                        walk_expr(b, "respond", out);
                    }
                    Stmt::InvokeMerge(_, args) => {
                        for (_, v) in args {
                            walk_expr(v, "invoke", out);
                        }
                    }
                    Stmt::If(arms, els) => {
                        for (c, _span, b) in arms {
                            walk_expr(c, "if", out);
                            walk_stmts(b, out);
                        }
                        if let Some(b) = els {
                            walk_stmts(b, out);
                        }
                    }
                    Stmt::For(_, it, b) => {
                        walk_expr(it, "for", out);
                        walk_stmts(b, out);
                    }
                    Stmt::Try(body, _, handler) => {
                        walk_stmts(body, out);
                        walk_stmts(handler, out);
                    }
                    Stmt::Remember(key, window) => {
                        walk_expr(key, "remember", out);
                        walk_expr(window, "remember", out);
                    }
                }
            }
        }
        walk_stmts(&prog.stmts, &mut prog.secret_refs);
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
                    let key = self.word("key")?;
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

    fn block(&mut self) -> Result<Block, String> {
        let mut block = Block::default();
        loop {
            self.skip_newlines();
            match self.peek() {
                Tok::End | Tok::Elif | Tok::Else => break,
                _ if self.at_catch() => break,
                _ => {}
            }
            if self.at_end() {
                return Err(self.err("missing `end`"));
            }
            let line = self.line();
            let stmt = self.stmt()?;
            block.push(line, stmt);
        }
        Ok(block)
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
            let key = self.word("argument name")?;
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
            Tok::Respond => {
                self.next();
                let status = self.expr(0)?;
                self.eat(Tok::Comma, ",")?;
                let body = self.expr(0)?;
                Ok(Stmt::Respond(status, body))
            }
            Tok::Invoke => {
                self.next();
                let (name, args) = self.invoke_args()?;
                Ok(Stmt::InvokeMerge(name, args))
            }
            Tok::If => {
                self.next();
                let mut arms = Vec::new();
                let cstart = self.pos();
                let cond = self.expr(0)?;
                let cend = self.pos(); // the ':' token position — condition is src[cstart..cend]
                self.eat(Tok::Colon, ":")?;
                arms.push((cond, (cstart, cend), self.block()?));
                let mut otherwise = None;
                loop {
                    match self.peek() {
                        Tok::Elif => {
                            self.next();
                            let cstart = self.pos();
                            let c = self.expr(0)?;
                            let cend = self.pos();
                            self.eat(Tok::Colon, ":")?;
                            arms.push((c, (cstart, cend), self.block()?));
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
            Tok::Ident(w) if w == "try" && self.peek2() == Some(&Tok::Colon) => {
                self.next();
                self.next();
                let body = self.block()?;
                if !self.at_catch() {
                    return Err(self.err("expected `catch:` or `catch e:` after the statements of `try`"));
                }
                self.next();
                let name = match self.peek().clone() {
                    Tok::Ident(name) => {
                        self.next();
                        Some(name)
                    }
                    _ => None,
                };
                self.eat(Tok::Colon, ":")?;
                let handler = self.block()?;
                self.eat(Tok::End, "end")?;
                Ok(Stmt::Try(body, name, handler))
            }
            Tok::Ident(_) if self.at_catch() => Err(self.err("`catch` without `try`")),
            Tok::Ident(w) if w == "remember" && !matches!(self.peek2(), Some(Tok::Eq | Tok::Dot)) => {
                self.next();
                let key = self.expr(0)?;
                self.eat(Tok::For, "for and a window, as in `remember key for \"7d\"`")?;
                let at = self.pos();
                let window = self.expr(0)?;
                match &window {
                    Expr::Lit(Value::String(text)) => {
                        window_of(text).map_err(|e| err_at(at, self.src, &e))?;
                    }
                    Expr::Var(name) if is_upper_name(name) => self.windows.push((name.clone(), at)),
                    _ => {
                        return Err(err_at(at, self.src, "the window of `remember` is a duration the flow writes, \"7d\", or a constant that holds one"))
                    }
                }
                Ok(Stmt::Remember(key, window))
            }
            Tok::Ident(first) => {
                self.next();
                let mut path = vec![first];
                while *self.peek() == Tok::Dot {
                    self.next();
                    path.push(self.word("field name")?);
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
                            let mut sub = Parser { toks: &toks, i: 0, src: self.src, newlines: self.newlines, windows: Vec::new() };
                            out.push(FStringPart::Expr(sub.expr(0)?));
                        }
                    }
                }
                Expr::FString(out)
            }
            Tok::Not => Expr::Unary(UnOp::Not, Box::new(self.expr(9)?)),
            Tok::Minus => Expr::Unary(UnOp::Neg, Box::new(self.expr(9)?)),
            // source/tool/driver are contextual directives (line-start only):
            // in expression position they are ordinary variable reads — an
            // invoke argument named `source` (EvidenceFact) must be readable.
            Tok::Source => Expr::Var("source".into()),
            Tok::Tool => Expr::Var("tool".into()),
            Tok::Driver => Expr::Var("driver".into()),
            Tok::Api => Expr::Var("api".into()),
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
                    let key = self.word("key")?;
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
                    let f = self.word("field name")?;
                    lhs = Expr::Field(Box::new(lhs), f, false);
                }
                Tok::QDot => {
                    self.next();
                    let f = self.word("field name")?;
                    lhs = Expr::Field(Box::new(lhs), f, true);
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
                for (_, _, b) in arms {
                    collect_static(b, consts, emits, invokes);
                }
                if let Some(b) = otherwise {
                    collect_static(b, consts, emits, invokes);
                }
            }
            Stmt::For(_, _, b) => collect_static(b, consts, emits, invokes),
            Stmt::Try(body, _, handler) => {
                collect_static(body, consts, emits, invokes);
                collect_static(handler, consts, emits, invokes);
            }
            _ => {}
        }
    }
}

impl Program {
    /// Does this flow write to the bus — directly via `emit`, or INDIRECTLY via a
    /// service `invoke` (a service can `emit`, and its emits are appended to this
    /// flow's at runtime)? This is the "is it a pure read?" test the write gate
    /// needs: a read-method /api route may run unauthenticated ONLY if it neither
    /// emits nor invokes ANYWHERE.
    ///
    /// It must be a full AST visitor, not `emit_subjects`/`invokes`: both are
    /// static-resolution lists (for the panel/graph) and UNDER-approximate — a
    /// dynamic emit subject (Finding A') and an `invoke` nested in a subexpression
    /// (Finding A'') are invisible to them. Conservative over-approximation: ANY
    /// emit or ANY invoke, wherever it sits, counts. A read flow that invokes a
    /// pure-formatting service is gated too (fail-closed — the operator adds the
    /// token); resolving invoked services transitively to un-gate those is a later
    /// refinement. Exhaustive matches (no `_`) so a new Stmt/Expr variant fails to
    /// compile here rather than silently escaping the gate.
    pub fn writes_bus(&self) -> bool {
        stmts_write_bus(&self.stmts)
    }

    /// Whether the code reads the variable `name` anywhere. A flow that reads
    /// `unreadable` takes the messages that are not JSON (ADR-0039).
    pub fn reads_var(&self, name: &str) -> bool {
        stmts_read_var(&self.stmts, name)
    }
}

fn stmts_read_var(stmts: &[Stmt], name: &str) -> bool {
    stmts.iter().any(|s| match s {
        Stmt::Assign(_, e) => expr_reads_var(e, name),
        Stmt::For(_, iter, body) => expr_reads_var(iter, name) || stmts_read_var(body, name),
        Stmt::If(arms, otherwise) => {
            arms.iter().any(|(cond, _, body)| expr_reads_var(cond, name) || stmts_read_var(body, name))
                || otherwise.as_deref().is_some_and(|body| stmts_read_var(body, name))
        }
        Stmt::Emit(a, b) | Stmt::Respond(a, b) | Stmt::Remember(a, b) => expr_reads_var(a, name) || expr_reads_var(b, name),
        Stmt::InvokeMerge(_, args) => args.iter().any(|(_, v)| expr_reads_var(v, name)),
        Stmt::Try(body, _, handler) => stmts_read_var(body, name) || stmts_read_var(handler, name),
    })
}

fn expr_reads_var(e: &Expr, name: &str) -> bool {
    match e {
        Expr::Var(n) => n == name,
        Expr::Lit(_) => false,
        Expr::FString(parts) => parts.iter().any(|p| matches!(p, FStringPart::Expr(e) if expr_reads_var(e, name))),
        Expr::Doc(pairs) | Expr::Invoke(_, pairs) => pairs.iter().any(|(_, e)| expr_reads_var(e, name)),
        Expr::Array(es) | Expr::Call(_, es) => es.iter().any(|e| expr_reads_var(e, name)),
        Expr::Field(base, _, _) => expr_reads_var(base, name),
        Expr::Index(a, b) | Expr::Filter(a, b) | Expr::Bin(_, a, b) => expr_reads_var(a, name) || expr_reads_var(b, name),
        Expr::Projection(b) | Expr::Unary(_, b) => expr_reads_var(b, name),
    }
}

fn stmts_write_bus(stmts: &[Stmt]) -> bool {
    stmts.iter().any(|s| match s {
        Stmt::Emit(_, _) => true,               // a direct bus write
        Stmt::InvokeMerge(_, _) => true,        // a service invoke — the service may emit
        Stmt::Assign(_, e) => expr_writes_bus(e),
        Stmt::Respond(status, body) => expr_writes_bus(status) || expr_writes_bus(body),
        Stmt::If(arms, otherwise) => {
            arms.iter().any(|(cond, _, body)| expr_writes_bus(cond) || stmts_write_bus(body))
                || otherwise.as_deref().map(stmts_write_bus).unwrap_or(false)
        }
        Stmt::For(_, iter, body) => expr_writes_bus(iter) || stmts_write_bus(body),
        Stmt::Try(body, _, handler) => stmts_write_bus(body) || stmts_write_bus(handler),
        // a mark that later runs read: a write (ADR-0039, point 4)
        Stmt::Remember(_, _) => true,
    })
}

/// True if evaluating `e` could invoke a service (which may emit). `Invoke` in any
/// position — an arg, a dict value, an f-string hole, a condition — counts.
fn expr_writes_bus(e: &Expr) -> bool {
    match e {
        Expr::Invoke(_, _) => true,
        Expr::Lit(_) | Expr::Var(_) => false,
        Expr::FString(parts) => parts.iter().any(|p| match p {
            FStringPart::Expr(e) => expr_writes_bus(e),
            FStringPart::Lit(_) => false,
        }),
        Expr::Doc(pairs) => pairs.iter().any(|(_, e)| expr_writes_bus(e)),
        Expr::Array(es) => es.iter().any(expr_writes_bus),
        Expr::Field(b, _, _) => expr_writes_bus(b),
        Expr::Index(a, b) => expr_writes_bus(a) || expr_writes_bus(b),
        Expr::Projection(b) => expr_writes_bus(b),
        Expr::Filter(a, b) => expr_writes_bus(a) || expr_writes_bus(b),
        Expr::Call(_, args) => args.iter().any(expr_writes_bus),
        Expr::Unary(_, b) => expr_writes_bus(b),
        Expr::Bin(_, a, b) => expr_writes_bus(a) || expr_writes_bus(b),
    }
}

pub fn parse(src: &str) -> Result<Program, String> {
    let toks = lex(src)?;
    let newlines: Vec<usize> = src.bytes().enumerate().filter(|(_, b)| *b == b'\n').map(|(i, _)| i).collect();
    let mut p = Parser { toks: &toks, i: 0, src, newlines: &newlines, windows: Vec::new() };
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
/// Resolution (package-scoped, with a strict coupling rule):
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
    cache: HashMap<String, Arc<Service>>,
    exports: HashMap<String, Vec<String>>,
    secrets: Arc<dyn crate::secrets::SecretStore>,
    depth: usize,
    /// While spans are exported: the hop a run belongs to and the `invoke`
    /// spans it left (ADR-0034). None otherwise, so an untraced run pays one
    /// test per invoke.
    trace: Option<InvokeTrace>,
    /// While the trace record is on: the statements the run goes through
    /// (ADR-0034, point 8). None otherwise, so a run pays one test per
    /// statement.
    steps: Option<Steps>,
    /// The service whose statements are running, for `steps`; None in the
    /// flow itself.
    in_service: Option<Arc<str>>,
    /// Whether an invoke of a service that answers reaches its system: never
    /// in a run that is shown (ADR-0032).
    calls: Calls,
    /// Why a call of the last run got no answer: what its message does next.
    failure: Option<crate::connectors::answer::Failure>,
    /// The trace a call carries when the run's invokes have no span.
    carried: Option<crate::trace::Context>,
    /// When the services and exports in the cache were loaded.
    loaded_at: std::time::Instant,
    /// The first call a run that is shown could not make, its answer not
    /// recorded: that run's outputs say nothing of the flow's.
    unrecorded: Option<String>,
    /// Whether a run of this engine has called a system.
    called: bool,
    /// The delivery of the message the run takes, when a later one may
    /// follow: a message on the bus. None for a request, which has none.
    delivery: Option<i64>,
    /// What `seen` reads: the keys the flow's runs remembered (ADR-0039).
    memory: Option<Arc<dyn Memory>>,
    /// A person released the held message (ADR-0040): its catch takes the
    /// failure now, whatever its deliveries.
    releasing: bool,
}

/// The keys a flow's runs remembered (ADR-0039, point 4), which `seen` reads.
/// The runtime writes a run's marks once its emits are stored.
pub trait Memory: Send + Sync {
    /// Whether a run remembered `key` and the window it gave has not passed.
    fn seen(&self, key: &str) -> Result<bool, String>;
}

/// The longest window `remember` takes.
pub const WINDOW_MAX: std::time::Duration = std::time::Duration::from_secs(3650 * 86_400);

/// The window `remember` gives a key: a whole number and its unit, seconds,
/// minutes, hours or days (`90s`, `30m`, `24h`, `7d`).
pub fn window_of(text: &str) -> Result<std::time::Duration, String> {
    let text = text.trim();
    let digits = text.find(|c: char| !c.is_ascii_digit()).unwrap_or(text.len());
    let (number, unit) = text.split_at(digits);
    let unit_secs: u64 = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        _ => return Err(format!("{text:?} is no window: a whole number and s, m, h or d, as \"7d\"")),
    };
    let secs = number
        .parse::<u64>()
        .ok()
        .filter(|&n| n > 0)
        .and_then(|n| n.checked_mul(unit_secs))
        .ok_or_else(|| format!("{text:?} is no window: a whole number above 0 and its unit, as \"7d\""))?;
    let window = std::time::Duration::from_secs(secs);
    if window > WINDOW_MAX {
        return Err(format!("{text:?} is longer than the longest window, 3650d"));
    }
    Ok(window)
}

/// The text of a key `seen` and `remember` take: a string as it is, another
/// value as JSON writes it. Null is no key.
fn key_of(value: &Value, what: &str) -> Result<String, String> {
    match value {
        Value::Null => Err(format!("{what} takes a key, and this one is null")),
        other => Ok(to_display(other)),
    }
}

/// Whether a run's calls reach a system (ADR-0032).
#[derive(Clone, Copy, PartialEq)]
enum Calls {
    Made,
    /// A run that is shown (a preview, a replay, the canary): it reads the
    /// answers recorded with the event, and calls nothing.
    Shown,
}

/// A service as the engine runs it: its statements, and the version of the
/// source they came from, which the trace record joins the code with.
struct Service {
    stmts: Block,
    /// A service that answers from a system instead of running statements.
    answers: Option<Declared>,
    /// `package:service`, as its steps name it.
    name: Arc<str>,
    /// Its source, hashed into its version the first time a run that records
    /// steps invokes it: a run that records none never pays for the hash.
    src: Box<str>,
    version: std::sync::OnceLock<Arc<str>>,
}

/// A service that answers from a system (ADR-0032): its driver, its settings
/// with their secrets resolved by the engine that loaded it (none for a run
/// that is shown), and what a check of its arguments reads.
struct Declared {
    driver: Arc<str>,
    config: crate::connectors::Config,
    /// Every setting the declaration names, resolved or not.
    settings: Vec<String>,
    /// Its URL, when the declaration writes it as a literal.
    url: Option<String>,
}

/// The settings a declaration names, and its URL when written as a literal.
fn declared_shape(prog: &Program) -> (Vec<String>, Option<String>) {
    let settings = prog
        .stmts
        .iter()
        .filter_map(|stmt| match stmt {
            Stmt::Assign(path, _) if path.len() == 1 && is_upper_name(&path[0]) => Some(path[0].clone()),
            _ => None,
        })
        .collect();
    let url = prog.surface.iter().find(|e| e.name == "URL").and_then(|e| e.value.as_str()).map(String::from);
    (settings, url)
}

/// Each invoke of `stmts`, with the names of its arguments.
fn invoke_sites(stmts: &[Stmt], out: &mut Vec<(String, Vec<String>)>) {
    fn site(name: &str, args: &[(String, Expr)], out: &mut Vec<(String, Vec<String>)>) {
        out.push((name.to_string(), args.iter().map(|(k, _)| k.clone()).collect()));
        args.iter().for_each(|(_, v)| expr(v, out));
    }
    fn expr(e: &Expr, out: &mut Vec<(String, Vec<String>)>) {
        match e {
            Expr::Invoke(name, args) => site(name, args, out),
            Expr::FString(parts) => parts.iter().for_each(|part| {
                if let FStringPart::Expr(e) = part {
                    expr(e, out)
                }
            }),
            Expr::Doc(pairs) => pairs.iter().for_each(|(_, v)| expr(v, out)),
            Expr::Array(items) | Expr::Call(_, items) => items.iter().for_each(|v| expr(v, out)),
            Expr::Field(b, _, _) | Expr::Projection(b) | Expr::Unary(_, b) => expr(b, out),
            Expr::Index(a, b) | Expr::Filter(a, b) | Expr::Bin(_, a, b) => {
                expr(a, out);
                expr(b, out);
            }
            Expr::Lit(_) | Expr::Var(_) => {}
        }
    }
    for stmt in stmts {
        match stmt {
            Stmt::Assign(_, e) => expr(e, out),
            Stmt::Emit(a, b) | Stmt::Respond(a, b) => {
                expr(a, out);
                expr(b, out);
            }
            Stmt::InvokeMerge(name, args) => site(name, args, out),
            Stmt::If(arms, otherwise) => {
                for (cond, _, body) in arms {
                    expr(cond, out);
                    invoke_sites(body, out);
                }
                if let Some(body) = otherwise {
                    invoke_sites(body, out);
                }
            }
            Stmt::For(_, iter, body) => {
                expr(iter, out);
                invoke_sites(body, out);
            }
            Stmt::Try(body, _, handler) => {
                invoke_sites(body, out);
                invoke_sites(handler, out);
            }
            Stmt::Remember(key, window) => {
                expr(key, out);
                expr(window, out);
            }
        }
    }
}

impl Service {
    fn version(&self) -> Arc<str> {
        self.version.get_or_init(|| crate::versions::hash_content(&self.src).into()).clone()
    }
}

/// How many statements of one run the record keeps: the first `STEPS_HEAD`
/// and the last `STEPS_TAIL`. A loop over a long array costs a bounded record,
/// and the statement a run failed on is always the last one kept.
pub const STEPS_HEAD: usize = 192;
pub const STEPS_TAIL: usize = 64;

/// The statements one run went through, in order (ADR-0034, point 8).
pub struct Steps {
    head: Vec<Step>,
    tail: std::collections::VecDeque<Step>,
    count: u64,
    /// Each service the run invoked, once, with the version of its source:
    /// its lines find their code by it even when the step where it started
    /// is among those left out.
    services: Vec<(Arc<str>, Arc<str>)>,
    /// A statement has said the run failed on it.
    failed: bool,
}

/// The services a run's steps name with their versions, at most.
pub const SERVICES_MAX: usize = 32;

/// Step buffers handed back, emptied, when their steps are dropped. A run's
/// steps are filled on its flow's thread and written on the record's: freed
/// there, a buffer costs a lock in glibc's malloc that the flow's thread
/// takes too, and that measured more than recording the steps.
static SPARE: std::sync::Mutex<Vec<Vec<Step>>> = std::sync::Mutex::new(Vec::new());
const SPARE_MAX: usize = 64;

impl Drop for Steps {
    fn drop(&mut self) {
        let mut head = std::mem::take(&mut self.head);
        if head.capacity() == 0 {
            return;
        }
        head.clear();
        if let Ok(mut spare) = SPARE.try_lock() {
            if spare.len() < SPARE_MAX {
                spare.push(head);
            }
        }
    }
}

/// A statement that ran, or an invoked service that started.
pub struct Step {
    /// The service the statement is in, `package:service`; None in the flow.
    pub service: Option<Arc<str>>,
    pub what: What,
}

pub enum What {
    /// A statement started on this line.
    Line(u32),
    /// The `emit` on this line sent to this subject.
    Emit(u32, String),
    /// The flow's `respond` on this line set this HTTP status: the caller
    /// gets the last one set, if the run succeeds.
    Respond(u32, u16),
    /// An invoked service started, at this version of its source.
    Enter(Arc<str>),
    /// The statement on this line failed, and a `catch` took the failure.
    Caught(u32),
}

impl Steps {
    pub(crate) fn new() -> Steps {
        let head = SPARE.try_lock().ok().and_then(|mut spare| spare.pop());
        Steps {
            head: head.unwrap_or_else(|| Vec::with_capacity(16)),
            tail: std::collections::VecDeque::new(),
            count: 0,
            services: Vec::new(),
            failed: false,
        }
    }

    /// A service the run invoked, at this version of its source.
    pub(crate) fn entered(&mut self, name: &Arc<str>, version: &Arc<str>) {
        if self.services.len() < SERVICES_MAX && !self.services.iter().any(|(n, _)| n == name) {
            self.services.push((name.clone(), version.clone()));
        }
    }

    /// Each service the run invoked, with the version of its source.
    pub fn services(&self) -> &[(Arc<str>, Arc<str>)] {
        &self.services
    }

    pub(crate) fn push(&mut self, step: Step) -> u64 {
        let k = self.count;
        self.count += 1;
        if self.head.len() < STEPS_HEAD {
            self.head.push(step);
        } else {
            if self.tail.len() == STEPS_TAIL {
                self.tail.pop_front();
            }
            self.tail.push_back(step);
        }
        k
    }

    /// The `k`th step pushed, if it is still kept.
    fn get_mut(&mut self, k: u64) -> Option<&mut Step> {
        if (k as usize) < self.head.len() {
            return self.head.get_mut(k as usize);
        }
        let first_in_tail = self.count - self.tail.len() as u64;
        if k >= first_in_tail {
            self.tail.get_mut((k - first_in_tail) as usize)
        } else {
            None
        }
    }

    /// The first steps kept, in order.
    pub fn head(&self) -> &[Step] {
        &self.head
    }

    /// The last steps kept, in order, after `skipped()` left out.
    pub fn tail(&self) -> impl Iterator<Item = &Step> {
        self.tail.iter()
    }

    /// How many steps ran between the head and the tail, not kept.
    pub fn skipped(&self) -> u64 {
        self.count - (self.head.len() + self.tail.len()) as u64
    }

    /// How many are kept.
    pub fn len(&self) -> usize {
        self.head.len() + self.tail.len()
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// A statement failed, and the run ended on it: its step is the last.
    pub fn failed(&self) -> bool {
        self.failed
    }

    #[cfg(test)]
    pub(crate) fn set_failed(&mut self) {
        self.failed = true;
    }

    /// The statements from the `first`th step on sent and answered nothing:
    /// a `catch` dropped what its `try` emitted or responded before the
    /// failure.
    fn dropped_since(&mut self, first: u64) {
        let first_in_tail = self.count - self.tail.len() as u64;
        let head = self.head.iter_mut().skip(first as usize);
        let tail = self.tail.iter_mut().skip(first.saturating_sub(first_in_tail) as usize);
        for step in head.chain(tail) {
            if let What::Emit(line, _) | What::Respond(line, _) = step.what {
                step.what = What::Line(line);
            }
        }
    }

    /// A `catch` took the failure the last step said: the run goes on.
    fn caught(&mut self) {
        if !std::mem::replace(&mut self.failed, false) {
            return;
        }
        let last = match self.tail.back_mut() {
            Some(step) => Some(step),
            None => self.head.last_mut(),
        };
        if let Some(step) = last {
            if let What::Line(line) = step.what {
                step.what = What::Caught(line);
            }
        }
    }
}

struct InvokeTrace {
    /// The hop, then each invoke in progress: the last is the parent of the next.
    stack: Vec<crate::trace::Hop>,
    done: Vec<InvokeSpan>,
}

/// One `invoke`, timed, under the hop or the invoke that made it.
pub struct InvokeSpan {
    pub hop: crate::trace::Hop,
    /// `package:service`.
    pub service: String,
    /// The service file, relative to the project root.
    pub file: String,
    pub start_nanos: u128,
    pub end_nanos: u128,
    pub error: Option<String>,
}

impl Engine {
    /// An engine for a REAL run: `secret()` yields the value. Only what performs
    /// a flow or a connector takes this; anything that returns a run's result to
    /// a caller takes `shadow`. There is deliberately no `new`, so that a new
    /// proof path cannot inherit real secrets by default.
    pub fn live(project_root: PathBuf, caller_pkg: String) -> Self {
        Self::with_secrets(project_root, caller_pkg, crate::secrets::default_store())
    }

    /// An engine for a SHADOW run: `secret()` stays fail-closed and yields a
    /// placeholder, never the value (see `secrets::RedactedStore`).
    pub fn shadow(project_root: PathBuf, caller_pkg: String) -> Self {
        let store = crate::secrets::RedactedStore::new(crate::secrets::default_store());
        let mut engine = Self::with_secrets(project_root, caller_pkg, Arc::new(store));
        engine.calls = Calls::Shown;
        engine
    }
    pub fn with_secrets(
        project_root: PathBuf,
        caller_pkg: String,
        secrets: Arc<dyn crate::secrets::SecretStore>,
    ) -> Self {
        Engine {
            project_root,
            caller_pkg,
            cache: HashMap::new(),
            exports: HashMap::new(),
            secrets,
            depth: 0,
            trace: None,
            steps: None,
            in_service: None,
            calls: Calls::Made,
            failure: None,
            carried: None,
            loaded_at: std::time::Instant::now(),
            unrecorded: None,
            called: false,
            delivery: None,
            memory: None,
            releasing: false,
        }
    }
    /// Call a service that answers (ADR-0032), carrying the trace of `hop`, the
    /// invoke's span, or else of the run. A run that is shown calls nothing: it
    /// says which arguments the service does not take, if any, and otherwise
    /// that the answer is not recorded.
    fn answer(
        &mut self,
        name: &str,
        declared: &Declared,
        args: &Map<String, Value>,
        hop: Option<&crate::trace::Hop>,
    ) -> Result<Map<String, Value>, String> {
        if self.calls == Calls::Shown {
            if let Some(url) = &declared.url {
                let given: Vec<&str> = args.keys().map(String::as_str).collect();
                let settings: Vec<&str> = declared.settings.iter().map(String::as_str).collect();
                crate::connectors::answer::fits(url, &given, &settings).map_err(|e| format!("{name}: {e}"))?;
            }
            let mut call: String = args.iter().map(|(k, v)| format!("{k}: {v}")).collect::<Vec<_>>().join(", ");
            if call.len() > 200 {
                let cut = (0..=200).rev().find(|&i| call.is_char_boundary(i)).unwrap_or(0);
                call.truncate(cut);
                call.push('…');
            }
            let call = format!("{name}({call})");
            let error = format!("{call} answers from a system, and a run that is shown calls none: no answer was recorded for it");
            self.unrecorded.get_or_insert(call);
            return Err(error);
        }
        self.called = true;
        let trace = hop.map(|hop| hop.context()).or_else(|| self.carried.clone());
        let config = &declared.config;
        crate::connectors::answer::call(name, &declared.driver, config, args, trace.as_ref()).map_err(|failure| {
            let error = failure.error.clone();
            self.failure = Some(failure);
            error
        })
    }
    /// The first call a run that is shown could not make, its answer not
    /// recorded (ADR-0032, point 4): what that run gave says nothing of the
    /// flow's outputs, and a comparison leaves its event out.
    pub fn take_unrecorded(&mut self) -> Option<String> {
        self.unrecorded.take()
    }
    /// Whether a run of this engine has called a system.
    pub fn made_a_call(&self) -> bool {
        self.called
    }
    /// The delivery of the message the runs that follow take, on the bus;
    /// None for a run no later delivery follows. A `catch` takes a failure
    /// that a later delivery may change only on the message's last one.
    pub fn delivered(&mut self, delivery: Option<i64>) {
        self.delivery = delivery;
    }
    /// Whether the runs that follow are the release of a held message
    /// (ADR-0040): a catch then takes any failure, as on a last delivery.
    pub fn releasing(&mut self, releasing: bool) {
        self.releasing = releasing;
    }
    /// What `seen` reads in the runs that follow.
    pub fn remember_in(&mut self, memory: Option<Arc<dyn Memory>>) {
        self.memory = memory;
    }
    /// Whether the flow's runs remembered `key`. A run that is shown reads no
    /// state: a key is new to it.
    fn seen(&mut self, key: &str) -> Result<bool, String> {
        if self.calls == Calls::Shown {
            return Ok(false);
        }
        let memory = self.memory.as_ref().ok_or("seen() reads what a flow on the bus or an api flow remembered: this run has no memory")?;
        memory.seen(key).map_err(|e| {
            // a bucket that did not answer may answer the next delivery
            let failure = crate::connectors::answer::Failure::unanswered(format!("seen(): {e}"));
            let error = failure.error.clone();
            self.failure = Some(failure);
            error
        })
    }
    /// The failure a `catch` takes, as its name holds it, or None when it
    /// takes none (ADR-0039, point 3). It takes any failure no later delivery
    /// may change: an error in the code, a call the service does not try
    /// again, or one on the delivery that spends its attempts. A call a later
    /// delivery may answer fails the run, so that the message comes back,
    /// and so does a call a run that is shown could not make.
    fn caught(&mut self, error: &str) -> Option<Value> {
        if self.unrecorded.is_some() {
            return None;
        }
        let attempt = self.delivery.unwrap_or(1);
        if let Some(f) = self.failure.as_ref().filter(|_| !self.releasing && self.delivery.is_some()) {
            // A flow on the bus holds its message while the system is down
            // (ADR-0040): no delivery is its last, until a person releases it.
            if f.holds() {
                return None;
            }
            if f.retry && attempt < f.attempts.min(vejas_connector::MAX_DELIVERIES) {
                return None;
            }
        }
        let failure = self.failure.take();
        if let Some(steps) = self.steps.as_mut() {
            steps.caught();
        }
        Some(json!({
            "error": error,
            "service": failure.as_ref().map(|f| f.service.as_str()).filter(|s| !s.is_empty()),
            "status": failure.as_ref().and_then(|f| f.status),
            "attempts": attempt,
        }))
    }
    /// Whether a run of `prog` may call a system: a service it invokes, or one
    /// those invoke, declares a driver (ADR-0032). Read from the files as they
    /// are now. A service that cannot be read fails its run without calling.
    pub fn reaches_system(&mut self, prog: &Program) -> bool {
        self.systems(prog).0
    }
    /// Whether a run of `prog` may call a service that holds its source while
    /// its system is down (ADR-0040): every declared service, unless it says
    /// `WHEN_DOWN = "dead-letter"`. Its flow then takes one message at a time
    /// across instances.
    pub fn holds_its_source(&mut self, prog: &Program) -> bool {
        self.systems(prog).1
    }
    /// Whether a run of `prog` may call a system, and whether one of those
    /// holds its source.
    fn systems(&mut self, prog: &Program) -> (bool, bool) {
        let caller = self.caller_pkg.clone();
        let mut seen: Vec<String> = Vec::new();
        let mut todo: Vec<(String, String)> = prog.invokes.iter().map(|n| (caller.clone(), n.clone())).collect();
        let mut reaches = false;
        let mut holds = false;
        while let Some((pkg, name)) = todo.pop() {
            self.caller_pkg = pkg;
            let Ok((pkg, svc)) = self.resolve(&name) else { continue };
            let key = format!("{pkg}:{svc}");
            if seen.contains(&key) {
                continue;
            }
            seen.push(key);
            let path = self.pkg_dir(&pkg).join("services").join(format!("{svc}.vjs"));
            let Ok(service) = std::fs::read_to_string(&path).map_err(|e| e.to_string()).and_then(|src| parse(&src)) else {
                continue;
            };
            if service.driver.is_some() {
                reaches = true;
                let down = service.surface.iter().find(|e| e.name == "WHEN_DOWN").and_then(|e| e.value.as_str());
                if down != Some("dead-letter") {
                    holds = true;
                    break;
                }
                continue;
            }
            todo.extend(service.invokes.iter().map(|n| (pkg.clone(), n.clone())));
        }
        self.caller_pkg = caller;
        (reaches, holds)
    }
    /// What a check of a flow tells of its invokes without running it: the
    /// arguments a service that answers does not take (ADR-0032, point 1). A
    /// service that cannot be found, or whose URL is not a literal, is left to
    /// the run.
    pub fn check_invokes(&mut self, prog: &Program) -> Vec<String> {
        let mut sites = Vec::new();
        invoke_sites(&prog.stmts, &mut sites);
        let mut problems = Vec::new();
        for (name, args) in sites {
            let Ok((pkg, svc)) = self.resolve(&name) else { continue };
            let path = self.pkg_dir(&pkg).join("services").join(format!("{svc}.vjs"));
            let Ok(service) = std::fs::read_to_string(&path).map_err(|e| e.to_string()).and_then(|src| parse(&src)) else {
                continue;
            };
            if service.driver.is_none() {
                continue;
            }
            if let Some(down) = service.surface.iter().find(|e| e.name == "WHEN_DOWN").map(|e| &e.value) {
                if !down.as_str().is_some_and(|d| crate::connectors::answer::WHEN_DOWN.contains(&d)) {
                    problems.push(format!("invoke {name}: its WHEN_DOWN is {down}, not \"hold\" or \"dead-letter\""));
                }
            }
            let (settings, url) = declared_shape(&service);
            let Some(url) = url else { continue };
            let given: Vec<&str> = args.iter().map(String::as_str).collect();
            let settings: Vec<&str> = settings.iter().map(String::as_str).collect();
            if let Err(e) = crate::connectors::answer::fits(&url, &given, &settings) {
                problems.push(format!("invoke {name}: {e}"));
            }
        }
        problems
    }
    /// Why a call of the last run got no answer, if one did not (ADR-0032).
    pub fn take_failure(&mut self) -> Option<crate::connectors::answer::Failure> {
        self.failure.take()
    }
    /// The trace the runs that follow carry to the systems they call, when
    /// their invokes have no span of their own.
    pub fn carry(&mut self, context: Option<crate::trace::Context>) {
        self.carried = context;
    }
    /// Record a span per `invoke` under `hop` in the runs that follow, or stop
    /// recording (None). What was recorded and not taken is dropped.
    pub fn trace_under(&mut self, hop: Option<&crate::trace::Hop>) {
        match (hop, &mut self.trace) {
            // Once per message: keep the allocations of the last one.
            (Some(hop), Some(t)) => {
                t.stack.clear();
                t.stack.push(hop.clone());
                t.done.clear();
            }
            (Some(hop), None) => {
                self.trace = Some(InvokeTrace { stack: vec![hop.clone()], done: Vec::new() })
            }
            (None, _) => self.trace = None,
        }
    }
    /// The `invoke` spans recorded since `trace_under`, oldest first.
    pub fn take_spans(&mut self) -> Vec<InvokeSpan> {
        self.trace.as_mut().map(|t| std::mem::take(&mut t.done)).unwrap_or_default()
    }
    /// Record the statements of the run that follows (ADR-0034, point 8), or
    /// stop recording. What was recorded and not taken is dropped.
    pub fn record_steps(&mut self, on: bool) {
        self.steps = on.then(Steps::new);
    }
    /// The statements recorded since `record_steps(true)`, in order.
    pub fn take_steps(&mut self) -> Option<Steps> {
        self.steps.take()
    }
    fn step(&mut self, what: What) -> Option<u64> {
        let steps = self.steps.as_mut()?;
        Some(steps.push(Step { service: self.in_service.clone(), what }))
    }
    /// The run failed on the statement of step `k`, on `line`. Unless a
    /// statement inside it failed first and said so, the steps end on it: an
    /// `invoke` in the statement may have added steps of its own after it.
    fn failed_on(&mut self, k: Option<u64>, line: u32) {
        let Some(steps) = self.steps.as_mut() else { return };
        if std::mem::replace(&mut steps.failed, true) {
            return;
        }
        if k.is_some_and(|k| steps.count != k + 1) {
            steps.push(Step { service: self.in_service.clone(), what: What::Line(line) });
        }
    }
    /// Say what the statement of step `k` did, once it has done it.
    fn mark(&mut self, k: Option<u64>, what: impl FnOnce(u32) -> What) {
        let step = k.and_then(|k| self.steps.as_mut().and_then(|s| s.get_mut(k)));
        if let Some(step) = step {
            if let What::Line(line) = step.what {
                step.what = what(line);
            }
        }
    }
    /// Forget the services and exports loaded `max_age` ago or more: an edit
    /// of a service, or of a secret a declaration reads, is picked up within
    /// that time, and a declaration's secrets are resolved at most once in it.
    pub fn refresh(&mut self, max_age: std::time::Duration) {
        if self.loaded_at.elapsed() >= max_age {
            self.cache.clear();
            self.exports.clear();
            self.loaded_at = std::time::Instant::now();
        }
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
    /// The names an invoked service gives a value to: they land in the
    /// caller's pipeline (the flow page says where a value comes from).
    pub fn service_makes(&mut self, name: &str) -> Result<Vec<String>, String> {
        let (pkg, svc) = self.resolve(name)?;
        let service = self.service(&pkg, &svc)?;
        if service.answers.is_some() {
            return Ok(["status", "body", "headers"].map(String::from).to_vec());
        }
        let mut own = Vec::new();
        collect_own(&service.stmts, &mut own);
        Ok(own)
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
    fn service(&mut self, pkg: &str, svc: &str) -> Result<Arc<Service>, String> {
        let key = format!("{pkg}:{svc}");
        if let Some(service) = self.cache.get(&key) {
            return Ok(service.clone());
        }
        let path = self.pkg_dir(pkg).join("services").join(format!("{svc}.vjs"));
        let src = std::fs::read_to_string(&path)
            .map_err(|_| format!("unknown service {key:?} (no {})", path.display()))?;
        let prog = parse(&src).map_err(|e| format!("service {key}: {e}"))?;
        let answers = match &prog.driver {
            None => None,
            Some(driver) if crate::connectors::answer::DRIVERS.contains(&driver.as_str()) => {
                // Its file holds settings. One that invoked a service would
                // call it each time it loads, itself included.
                if prog.writes_bus() {
                    return Err(format!(
                        "service {key} declares driver {driver:?}: its file holds settings, and invokes and emits nothing"
                    ));
                }
                let (settings, url) = declared_shape(&prog);
                // Its settings are its UPPERCASE names, evaluated as a
                // connector's manifest is: secret() resolves here. A run that
                // is shown calls nothing, so it resolves nothing either.
                let config = match self.calls {
                    Calls::Shown => Map::new(),
                    Calls::Made => {
                        let mut resolving =
                            Engine::with_secrets(self.project_root.clone(), pkg.to_string(), self.secrets.clone());
                        match run(&prog, &Value::Object(Map::new()), &mut resolving) {
                            Ok(ctx) => ctx.vars.into_iter().filter(|(k, _)| is_upper_name(k)).collect(),
                            // a store that did not answer may answer a later
                            // delivery; a secret it does not have will not
                            Err(e) => {
                                let later = resolving.failure.is_some();
                                let failure =
                                    crate::connectors::answer::Failure::unresolved(&key, format!("service {key}: {e}"), later);
                                let error = failure.error.clone();
                                self.failure = Some(failure);
                                return Err(error);
                            }
                        }
                    }
                };
                Some(Declared {
                    driver: Arc::from(driver.as_str()),
                    config: crate::connectors::Config(config),
                    settings,
                    url,
                })
            }
            Some(driver) => {
                return Err(format!(
                    "service {key} declares driver {driver:?}, which answers no call: a service declares {}",
                    crate::connectors::answer::DRIVERS.join(", ")
                ))
            }
        };
        let service = Arc::new(Service {
            answers,
            stmts: prog.stmts,
            name: Arc::from(key.as_str()),
            src: src.into_boxed_str(),
            version: std::sync::OnceLock::new(),
        });
        self.cache.insert(key, service.clone());
        Ok(service)
    }
}

pub struct Ctx {
    pub vars: Map<String, Value>,
    pub emits: Vec<(String, Value)>,
    /// Set by `respond <status>, <body>` in an `api` flow: the HTTP response.
    pub response: Option<(Value, Value)>,
    /// What the run remembers, each key with its window: the runtime marks
    /// them once the run's emits are stored (ADR-0039, point 4).
    pub marks: Vec<(String, std::time::Duration)>,
}

pub fn run(prog: &Program, event: &Value, engine: &mut Engine) -> Result<Ctx, String> {
    run_with(prog, event, Map::new(), engine)
}

/// [`run`], with variables of the run's own over the event's fields: an `api`
/// flow's path parameters and its `request` (ADR-0039).
pub fn run_with(prog: &Program, event: &Value, given: Map<String, Value>, engine: &mut Engine) -> Result<Ctx, String> {
    engine.failure = None;
    let mut vars = Map::new();
    if let Value::Object(obj) = event {
        for (k, v) in obj {
            vars.insert(k.clone(), v.clone());
        }
    }
    vars.extend(given);
    vars.insert("event".into(), event.clone());
    let mut ctx = Ctx { vars, emits: Vec::new(), response: None, marks: Vec::new() };
    exec_block(&prog.stmts, &mut ctx, engine)?;
    Ok(ctx)
}

fn run_service(name: &str, args: Map<String, Value>, engine: &mut Engine, caller: &mut Ctx) -> Result<Value, String> {
    if engine.depth >= 32 {
        return Err(format!("invoke depth exceeded at {name}"));
    }
    let (pkg, svc) = engine.resolve(name)?;
    engine.depth += 1;
    let service = match engine.service(&pkg, &svc) {
        Ok(s) => s,
        Err(e) => {
            engine.depth -= 1;
            // a failure names the service as the flow does, as a call's does
            if let Some(failure) = engine.failure.as_mut() {
                failure.service = name.to_string();
            }
            return Err(e);
        }
    };
    // The steps that follow are this service's, until it returns.
    let saved_service = engine.steps.as_mut().map(|steps| {
        let version = service.version();
        steps.entered(&service.name, &version);
        steps.push(Step { service: Some(service.name.clone()), what: What::Enter(version) });
        engine.in_service.replace(service.name.clone())
    });
    // A span for this invoke, under whatever is running it (ADR-0034).
    let span = engine.trace.as_mut().map(|t| {
        let hop = t.stack.last().expect("the hop is at the bottom").child();
        t.stack.push(hop.clone());
        (hop, crate::metrics::now_nanos())
    });
    let names = span.is_some().then(|| {
        (format!("{pkg}:{svc}"), engine.pkg_dir(&pkg).join("services").join(format!("{svc}.vjs")))
    });
    // The invoked service's own invokes resolve within ITS package.
    let saved_pkg = std::mem::replace(&mut engine.caller_pkg, pkg);
    let mut ctx = Ctx { vars: Map::new(), emits: Vec::new(), response: None, marks: Vec::new() };
    let result = match &service.answers {
        Some(declared) => {
            let hop = span.as_ref().map(|(hop, _)| hop);
            // What the flow passed comes back with the answer, as it does from
            // a service of code: a merge gives the flow its arguments too.
            engine.answer(name, declared, &args, hop).map(|answer| {
                ctx.vars = args;
                ctx.vars.extend(answer);
            })
        }
        None => {
            ctx.vars = args.clone();
            ctx.vars.insert("event".into(), Value::Object(args));
            exec_block(&service.stmts, &mut ctx, engine)
        }
    };
    engine.caller_pkg = saved_pkg;
    if let Some(saved) = saved_service {
        engine.in_service = saved;
    }
    engine.depth -= 1;
    if let (Some((hop, start_nanos)), Some((service, file))) = (span, names) {
        let file = file.strip_prefix(&engine.project_root).unwrap_or(&file).display().to_string();
        let t = engine.trace.as_mut().expect("a span was opened, so the engine traces");
        t.stack.pop();
        t.done.push(InvokeSpan {
            hop,
            service,
            file,
            start_nanos,
            end_nanos: crate::metrics::now_nanos(),
            error: result.as_ref().err().cloned(),
        });
    }
    result?;
    caller.emits.append(&mut ctx.emits);
    caller.marks.append(&mut ctx.marks);
    ctx.vars.remove("event");
    Ok(Value::Object(ctx.vars))
}

fn exec_block(block: &Block, ctx: &mut Ctx, engine: &mut Engine) -> Result<(), String> {
    for (stmt, &line) in block.stmts.iter().zip(&block.lines) {
        let step = if line > 0 { engine.step(What::Line(line)) } else { None };
        if let Err(e) = exec_stmt(stmt, step, ctx, engine) {
            engine.failed_on(step, line);
            return Err(e);
        }
    }
    Ok(())
}

/// One statement; `step` is its own, if the run records steps.
fn exec_stmt(stmt: &Stmt, step: Option<u64>, ctx: &mut Ctx, engine: &mut Engine) -> Result<(), String> {
    match stmt {
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
            let out = run_service(name, arg_map, engine, ctx)?;
            if let Value::Object(o) = out {
                for (k, v) in o {
                    ctx.vars.insert(k, v);
                }
            }
        }
        Stmt::If(arms, otherwise) => {
            let mut done = false;
            for (cond, _span, body) in arms {
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
            if !valid_subject(&s) {
                return Err(format!("emit subject {s:?} is not a valid subject"));
            }
            let p = eval(payload, ctx, engine)?;
            engine.mark(step, |line| What::Emit(line, s.clone()));
            ctx.emits.push((s, p));
        }
        Stmt::Try(body, name, handler) => {
            // what the statements of the try sent, remembered and answered
            // is dropped with their failure: a run's effects stay all or none
            let (emits, marks, response) = (ctx.emits.len(), ctx.marks.len(), ctx.response.clone());
            let first = engine.steps.as_ref().map(|steps| steps.count);
            if let Err(error) = exec_block(body, ctx, engine) {
                let failure = engine.caught(&error).ok_or(error)?;
                ctx.emits.truncate(emits);
                ctx.marks.truncate(marks);
                ctx.response = response;
                if let (Some(first), Some(steps)) = (first, engine.steps.as_mut()) {
                    steps.dropped_since(first);
                }
                if let Some(name) = name {
                    ctx.vars.insert(name.clone(), failure);
                }
                exec_block(handler, ctx, engine)?;
            }
        }
        Stmt::Remember(key, window) => {
            let key = key_of(&eval(key, ctx, engine)?, "remember")?;
            let window = match eval(window, ctx, engine)? {
                Value::String(text) => window_of(&text)?,
                other => return Err(format!("the window of remember is a duration, as \"7d\": got {other}")),
            };
            ctx.marks.push((key, window));
        }
        Stmt::Respond(status, body) => {
            let st = eval(status, ctx, engine)?;
            let b = eval(body, ctx, engine)?;
            // a service's `respond` answers no one: run_service drops it
            if engine.in_service.is_none() {
                let code = http_status(&st);
                engine.mark(step, |line| What::Respond(line, code));
            }
            ctx.response = Some((st, b));
        }
    }
    Ok(())
}

/// The HTTP status a `respond` sets: a number, or a string that
/// holds one, and 200 otherwise.
pub fn http_status(status: &Value) -> u16 {
    status.as_u64().or_else(|| status.as_str().and_then(|s| s.parse().ok())).unwrap_or(200) as u16
}

/// Can this string go on the wire as a subject? The NATS protocol is line-based
/// and space-separated, so a line break in a subject is a second protocol
/// command, and a subject built from an event field hands that to whoever sends
/// the event. Checked where a flow emits: the run fails, the message takes the
/// poison path to the DLQ, and a shadow run shows the error a live run would get.
pub fn valid_subject(subject: &str) -> bool {
    !subject.is_empty() && !subject.chars().any(|c| c.is_whitespace() || c.is_control())
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
            run_service(name, arg_map, engine, ctx)?
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
                let keep = eval(cond, ctx, engine);
                // the flow's own variables come back, also when the condition
                // failed: a catch runs with them
                for (k, old) in saved {
                    match old {
                        Some(v) => ctx.vars.insert(k, v),
                        None => ctx.vars.remove(&k),
                    };
                }
                if truthy(&keep?) {
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
            if name == "seen" {
                let key = key_of(vals.first().unwrap_or(&Value::Null), "seen()")?;
                Value::Bool(engine.seen(&key)?)
            } else if name == "secret" {
                // resolve a secret at run time; never a literal, never exposed.
                // fail closed: a missing secret aborts the run.
                let reference = vals
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or("secret() needs a string reference, e.g. secret(\"slack/webhook\")")?;
                let value = engine.secrets.read(reference).map_err(|missing| {
                    if !missing.later {
                        return missing.error;
                    }
                    // a store that did not answer may answer the next
                    // delivery: a catch waits for the last (ADR-0039)
                    let failure = crate::connectors::answer::Failure::unanswered(missing.error);
                    let error = failure.error.clone();
                    engine.failure = Some(failure);
                    error
                })?;
                Value::String(value)
            } else {
                builtin(name, &vals)?
            }
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
        // The base64 of a text's UTF-8 bytes (ADR-0039): a value that is not
        // a string is read as `str` gives it, and null stays null.
        "base64" => match arg(0) {
            Value::Null => Value::Null,
            v => {
                let text = to_display(&v);
                let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &text);
                // a credential in it is masked in its base64 as in the clear
                crate::secrets::derived(&text, &encoded);
                Value::String(encoded)
            }
        },
        _ => return Err(format!("unknown function {name}()")),
    })
}

// ───────────────────────────── editing ─────────────────────────────

/// Rewrite one surface literal in place (whole constant with key "-", or one
/// entry of a top-level doc literal). The result must re-parse.
/// The current value of a business-surface literal — the whole constant when
/// `key` is empty/"-", or one table entry. Returns None if the literal or key is
/// absent. Used to capture the `before` value for the promote audit trail
/// (ADR-0018) and to recover the previous value on rollback.
pub fn get_literal(src: &str, name: &str, key: &str) -> Option<Value> {
    let prog = parse(src).ok()?;
    let entry = prog.surface.iter().find(|e| e.name == name)?;
    if key.is_empty() || key == "-" {
        Some(entry.value.clone())
    } else {
        entry.value.get(key).cloned()
    }
}

pub fn set_literal(src: &str, name: &str, key: &str, new_value: &Value) -> Result<String, String> {
    let prog = parse(src)?;
    let entry = prog
        .surface
        .iter()
        .find(|e| e.name == name)
        .ok_or_else(|| format!("no literal assignment {name:?}"))?;
    let span = if key.is_empty() || key == "-" {
        Some(entry.value_span)
    } else {
        entry
            .entry_spans
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, span)| *span)
    };
    if let Some((start, end)) = span {
        // Replace an existing value (whole constant, or one table entry).
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
        return Ok(out);
    }
    // The key does not exist yet. Adding an entry to an existing table is a
    // DATA extension (N1, ADR-0019) — a new transcoding row, not a structural
    // change — so the expert does it from the panel without an agent. Only a
    // dict/table can gain a key; a constant or list cannot.
    let obj = entry
        .value
        .as_object()
        .ok_or_else(|| format!("{name} has no key {key:?} and is not a table — cannot add a key"))?;
    // The table's own closing brace is its last `}` token: a brace in one of
    // its strings, or in a comment after it, is no token.
    let (vstart, vend) = entry.value_span;
    let close = lex(src)?
        .iter()
        .filter(|t| (vstart..vend).contains(&t.pos) && matches!(t.tok, Tok::RBrace))
        .map(|t| t.pos)
        .last()
        .ok_or_else(|| format!("{name}: malformed table literal"))?;
    let k = serde_json::to_string(&Value::String(key.to_string())).unwrap();
    let v = serde_json::to_string(new_value).map_err(|e| e.to_string())?;
    // span-exact insert before the closing brace; no leading comma into an empty {}
    let insertion = if obj.is_empty() {
        format!("{k}: {v}")
    } else {
        format!(", {k}: {v}")
    };
    let mut out = String::with_capacity(src.len() + insertion.len());
    out.push_str(&src[..close]);
    out.push_str(&insertion);
    out.push_str(&src[close..]);
    parse(&out)?;
    Ok(out)
}

// ───────────────────────────── tests ─────────────────────────────

#[cfg(test)]
mod tests {
    #[test]
    fn what_a_branch_may_leave_to_the_event_is_read_from_it() {
        let prog = super::parse("source \"vx.a\"\nif urgent:\n  password = \"a\"\nend\nemit \"vx.o\", {p: password}\n").unwrap();
        assert_eq!(super::event_reads(&prog), ["urgent", "password"]);
        let prog = super::parse("source \"vx.a\"\nK = \"id\"\nemit \"vx.o\", {v: event[K]}\n").unwrap();
        assert_eq!(super::event_reads(&prog), ["event"]);
    }

    use super::*;
    use serde_json::json;

    fn eval_flow(src: &str, event: Value) -> Result<Vec<(String, Value)>, String> {
        let prog = parse(src)?;
        let mut engine = Engine::live(std::env::temp_dir(), "default".into());
        run(&prog, &event, &mut engine).map(|c| c.emits)
    }

    #[test]
    fn a_flow_says_whether_it_reads_a_variable() {
        let reads = |src: &str| parse(src).unwrap().reads_var("unreadable");
        assert!(reads("source \"vx.a\"\nif unreadable:\n  emit \"vx.bad\", {raw: unreadable.base64}\nend\n"));
        assert!(reads("source \"vx.a\"\nfor x in [1]:\n  emit f\"vx.{unreadable?.error}\", {}\nend\n"));
        assert!(reads("source \"vx.a\"\ninvoke log(m: unreadable)\n"));
        assert!(!reads("source \"vx.a\"\nemit \"vx.o\", {u: \"unreadable\", v: event.unreadable_x}\n"));
    }

    #[test]
    fn a_run_given_variables_keeps_its_event_as_sent() {
        let prog = parse("api \"PUT /orders/{id}\"\nrespond 200, {id: id, body_id: event.id, n: len(event), page: request.query.page}\n").unwrap();
        let mut engine = Engine::live(std::env::temp_dir(), "default".into());
        let given = json!({"id": "from-path", "request": {"query": {"page": "2"}}});
        let Value::Object(given) = given else { unreachable!() };
        let ctx = run_with(&prog, &json!({"id": "from-body", "x": 1}), given, &mut engine).unwrap();
        let (_, body) = ctx.response.unwrap();
        assert_eq!(body, json!({"id": "from-path", "body_id": "from-body", "n": 2, "page": "2"}));
        // an array is the event as it came
        let prog = parse("api \"POST /bulk\"\nrespond 200, {n: len(event), first: event[0].id}\n").unwrap();
        let ctx = run(&prog, &json!([{"id": "a"}, {"id": "b"}]), &mut engine).unwrap();
        assert_eq!(ctx.response.unwrap().1, json!({"n": 2, "first": "a"}));
    }

    #[test]
    fn rules_projection_faithful_or_raw() {
        let src = "source \"vx.orders.in\"\n\
                   MIN_TOTAL_EUR = 1000\n\
                   if event.total >= MIN_TOTAL_EUR:\n\
                     emit \"vx.erp.dispatch\", { id: event.id }\n\
                   elif event.total > 0 and event.status == \"OPEN\":\n\
                     emit \"vx.review.queue\", { id: event.id }\n\
                   else:\n\
                     emit \"vx.orders.reject\", { id: event.id }\n\
                   end\n";
        let prog = parse(src).unwrap();
        let rules = flow_rules(&prog, src, &prog.surface);
        assert_eq!(rules.len(), 3);
        // arm 1: a single comparison against a surface literal → projectable
        assert_eq!(rules[0]["projectable"], true);
        assert_eq!(rules[0]["when"], "event.total ≥ MIN_TOTAL_EUR");
        assert_eq!(rules[0]["then"][0], "→ vx.erp.dispatch");
        assert_eq!(rules[0]["literals"][0], "MIN_TOTAL_EUR");
        // arm 2: composed (and) → RAW, verbatim source, no paraphrase
        assert_eq!(rules[1]["projectable"], false);
        assert_eq!(rules[1]["raw"], "event.total > 0 and event.status == \"OPEN\"");
        // else: simple action → projectable, no condition
        assert_eq!(rules[2]["kind"], "else");
        assert_eq!(rules[2]["projectable"], true);
        assert_eq!(rules[2]["then"][0], "→ vx.orders.reject");
    }

    #[test]
    fn rules_resolve_emit_subject_from_surface_constant() {
        // The showcase flows emit to a subject held in a surface constant
        // (`emit OUT, …`). That is statically resolvable, so the arm must
        // project, not fall to RAW (the peer's edge case).
        let src = "source \"vx.orders.in\"\n\
                   MIN_TOTAL_EUR = 1000\n\
                   OUT = \"vx.bench.out\"\n\
                   if event.total >= MIN_TOTAL_EUR:\n\
                     tag = \"big\"\n\
                     emit OUT, { id: event.id, tag: tag }\n\
                   end\n";
        let prog = parse(src).unwrap();
        let rules = flow_rules(&prog, src, &prog.surface);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["projectable"], true, "constant subject must project");
        // resolved to the constant's value, and a pure assign before the emit is fine
        assert_eq!(rules[0]["then"][0], "→ vx.bench.out");
        assert_eq!(rules[0]["literals"][0], "MIN_TOTAL_EUR");
    }
    // The flow page reads a flow top to bottom: what comes in, how each value
    // is made, what it checks, where it sends.
    const SHAPE_SRC: &str = "# flow: order_sync\n\
source \"vx.shop.orders\"\n\
\n\
# The shop sends country names (\"France\"); the ERP wants ISO codes (\"FR\").\n\
COUNTRY_CODES = {\"France\": \"FR\", \"Germany\": \"DE\"}\n\
\n\
MIN_TOTAL_EUR = 0\n\
ERP_QUEUE = \"vx.erp.orders.upsert\"\n\
\n\
order_ref = split(id, \"#\")[1]\n\
customer_email = lower(email)\n\
total_eur = num(total_price)\n\
country = COUNTRY_CODES[shipping_address.country] ?? shipping_address.country\n\
\n\
# project order lines into ERP positions\n\
positions = []\n\
for line in line_items:\n\
    positions = positions + [{sku: line.sku, qty: num(line.quantity)}]\n\
end\n\
\n\
if total_eur >= MIN_TOTAL_EUR:\n\
    emit ERP_QUEUE, {order_ref: order_ref, country: country, positions: positions}\n\
end\n";

    #[test]
    fn a_flow_shape_reads_the_event_then_each_value_then_the_sends() {
        let prog = parse(SHAPE_SRC).unwrap();
        let shape = flow_shape(&prog, SHAPE_SRC, &[]);
        assert_eq!(
            shape["reads"],
            json!(["id", "email", "total_price", "shipping_address.country", "line_items"]),
            "what the flow reads from its event, in the order it reads it; never a name it assigns or a loop variable"
        );
        let steps = shape["steps"].as_array().unwrap();
        let kinds: Vec<&str> = steps.iter().map(|s| s["kind"].as_str().unwrap()).collect();
        // no step for the literals, nor for the empty start of what the loop builds
        assert_eq!(kinds, ["set", "set", "set", "set", "each", "when"]);
        assert_eq!(steps[0]["name"], "order_ref");
        assert_eq!(steps[0]["code"], "split(id, \"#\")[1]");
        assert_eq!(steps[0]["line"], 10);
        assert_eq!(steps[0]["uses"], json!(["id"]));
        assert_eq!(steps[3]["name"], "country");
        assert_eq!(steps[3]["literals"], json!(["COUNTRY_CODES"]), "the literal it uses, to edit in place");
        assert_eq!(steps[3]["uses"], json!(["shipping_address.country"]));
        assert_eq!(steps[4]["var"], "line");
        assert_eq!(steps[4]["over"], "line_items");
        assert_eq!(steps[4]["builds"], json!(["positions"]));
        assert_eq!(steps[4]["literals"], json!([]));
        assert_eq!(steps[5]["line"], 21);
        let sends = shape["sends"].as_array().unwrap();
        assert_eq!(sends.len(), 1);
        assert_eq!(sends[0]["subject"], "vx.erp.orders.upsert");
        assert_eq!(sends[0]["constant"], "ERP_QUEUE");
        assert_eq!(sends[0]["inside"], "when");
        assert_eq!(sends[0]["fields"][0], json!({"name": "order_ref", "from": "order_ref"}));
        assert_eq!(
            shape["docs"],
            json!({"COUNTRY_CODES": "The shop sends country names (\"France\"); the ERP wants ISO codes (\"FR\")."}),
            "a literal's comment is its label; a blank line or the file header ends it"
        );
    }

    #[test]
    fn a_literal_a_loop_uses_belongs_to_the_loop() {
        let src = "source \"vx.in\"\nUNITS = {\"kg\": 1000}\nout = []\nfor l in lines:\n  out = out + [l.qty * UNITS[l.unit]]\nend\n";
        let prog = parse(src).unwrap();
        let shape = flow_shape(&prog, src, &[]);
        assert_eq!(shape["steps"][0]["kind"], "each");
        assert_eq!(shape["steps"][0]["literals"], json!(["UNITS"]));
        assert_eq!(shape["reads"], json!(["lines"]), "a loop variable's fields are no event field");
    }

    #[test]
    fn a_value_an_invoked_service_makes_is_no_event_field() {
        let src = "source \"vx.in\"\ninvoke fmt(sev: severity)\nemit \"vx.out\", {text: alert_text}\n";
        let prog = parse(src).unwrap();
        let shape = flow_shape(&prog, src, &[("fmt".into(), vec!["alert_text".into()])]);
        assert_eq!(shape["reads"], json!(["severity"]));
        assert_eq!(shape["made_by"], json!({"alert_text": "fmt"}));
        let src = "source \"vx.in\"\nGREETING = \"hi\"\ninvoke fmt(text: f\"{GREETING} {origin}\")\n";
        let prog = parse(src).unwrap();
        let shape = flow_shape(&prog, src, &[]);
        assert_eq!(shape["steps"][0]["kind"], "invoke");
        assert_eq!(shape["steps"][0]["literals"], json!(["GREETING"]), "a literal an invoke passes is editable where it is passed");
        assert_eq!(shape["steps"][0]["uses"], json!(["origin"]));
    }

    #[test]
    fn a_field_the_flow_rewrites_is_still_read_from_the_event() {
        let src = "source \"vx.in\"\nemail = lower(email)\ncountry = country ?? \"FR\"\nemit \"vx.out\", {email: email, country: country}\n";
        let prog = parse(src).unwrap();
        assert_eq!(flow_shape(&prog, src, &[])["reads"], json!(["email", "country"]));
    }

    #[test]
    fn what_a_branch_sets_counts_after_it_not_in_the_next_branch() {
        let src = "source \"vx.in\"\nif big:\n  label = \"big\"\nelif small:\n  label = label\nelse:\n  label = \"mid\"\nend\nemit \"vx.out\", {label: label}\n";
        let prog = parse(src).unwrap();
        assert_eq!(flow_shape(&prog, src, &[])["reads"], json!(["big", "small", "label"]),
                   "the elif reads the event's label: the first branch did not run");
    }

    #[test]
    fn a_filter_reads_its_items_keys_not_the_event() {
        let src = "source \"vx.in\"\nMIN_QTY = 10\nbig = line_items[qty > MIN_QTY]\nemit \"vx.out\", {big: big}\n";
        let prog = parse(src).unwrap();
        let shape = flow_shape(&prog, src, &[]);
        assert_eq!(shape["reads"], json!(["line_items"]));
        assert_eq!(shape["steps"][0]["literals"], json!(["MIN_QTY"]));
        // what the condition reads through `event.` is the event's
        let src = "source \"vx.in\"\nbig = line_items[qty > event.min_qty and sku in event.wanted]\nemit \"vx.out\", {big: big}\n";
        let prog = parse(src).unwrap();
        assert_eq!(flow_shape(&prog, src, &[])["reads"], json!(["line_items", "min_qty", "wanted"]));
    }

    #[test]
    fn setting_one_field_leaves_the_others_read_from_the_event() {
        let src = "source \"vx.in\"\ncustomer.email = lower(customer.email)\nname = customer.name\nemit \"vx.out\", {email: customer.email, name: name, all: customer}\n";
        let prog = parse(src).unwrap();
        assert_eq!(flow_shape(&prog, src, &[])["reads"], json!(["customer.email", "customer.name", "customer"]));
        // what it set it does not read, and the rest of `customer` stays unread
        let src = "source \"vx.in\"\ncustomer.email = \"x@y\"\nemit \"vx.out\", {email: customer.email}\n";
        let prog = parse(src).unwrap();
        assert_eq!(flow_shape(&prog, src, &[])["reads"], json!([]));
        let src = "source \"vx.in\"\ncustomer.email = lower(customer.email)\nemit \"vx.out\", {email: customer.email}\n";
        let prog = parse(src).unwrap();
        assert_eq!(flow_shape(&prog, src, &[])["reads"], json!(["customer.email"]));
    }

    #[test]
    fn what_an_invoke_passes_comes_back_to_the_flow() {
        let src = "source \"vx.in\"\ninvoke fmt(text: f\"{id}\")\nemit \"vx.out\", {text: text}\n";
        let prog = parse(src).unwrap();
        assert_eq!(flow_shape(&prog, src, &[])["reads"], json!(["id"]));
        let src = "source \"vx.in\"\nemit \"vx.early\", {t: alert_text}\ninvoke fmt(sev: severity)\nemit \"vx.out\", {t: alert_text}\n";
        let prog = parse(src).unwrap();
        let shape = flow_shape(&prog, src, &[("fmt".into(), vec!["alert_text".into()])]);
        assert_eq!(shape["reads"], json!(["alert_text", "severity"]), "read before the invoke, it is the event's");
    }

    #[test]
    fn a_literal_doc_joins_its_comment_lines_and_stops_at_a_blank_line() {
        let src = "# flow: alerts\n# Only payments strictly above this amount\n#   raise a Slack alert.\nALERT = 250\n\n# unrelated\n\nOTHER = 1\n";
        let prog = parse(src).unwrap();
        let doc = |name: &str| {
            let e = prog.surface.iter().find(|e| e.name == name).unwrap();
            literal_doc(src, e.value_span.0)
        };
        assert_eq!(doc("ALERT").as_deref(), Some("Only payments strictly above this amount raise a Slack alert."));
        assert_eq!(doc("OTHER"), None, "a comment cut off by a blank line belongs to nothing");
    }

    #[test]
    fn a_flow_that_reads_its_event_by_name_lists_the_paths_under_it() {
        let src = "source \"vx.in\"\nif event.total > 10 and event.customer.tier == \"gold\":\n  emit \"vx.out\", event\nend\n";
        let prog = parse(src).unwrap();
        let shape = flow_shape(&prog, src, &[]);
        assert_eq!(shape["reads"], json!(["total", "customer.tier"]));
        assert_eq!(shape["sends"][0]["whole"], "event");
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
    fn keywords_as_keys_and_fields() {
        // `source`, `tool`, `in`, `end`… are valid doc keys and field names
        // (an EvidenceFact literally has a `source` field) — never variables.
        let p = one_emit(
            "fact = {source: \"graph\", tool: 1, in: 2, end: doc.source}\nemit \"vx.t\", fact",
            json!({"doc": {"source": "s1"}}),
        );
        assert_eq!(p, json!({"source": "graph", "tool": 1, "in": 2, "end": "s1"}));
        // null-safe field access on a keyword-named field
        let q = one_emit("emit \"vx.t\", {a: e?.driver ?? \"none\"}", json!({"e": {}}));
        assert_eq!(q["a"], "none");
        // assignment path segments too
        let r = one_emit("d.source = 7\nemit \"vx.t\", d", json!({}));
        assert_eq!(r, json!({"source": 7}));
        // surface literal with a keyword key keeps working (span-based editing)
        let prog = parse("T = {\"end\": \"x\", source: \"y\"}\n").unwrap();
        assert_eq!(prog.surface[0].value, json!({"end": "x", "source": "y"}));
        // source/tool/driver are READABLE as variables (an invoke arg is
        // literally named `source` in the evidence contract), incl. f-strings
        let s = one_emit(
            "emit \"vx.t\", {k: f\"{source}|x\", v: source, t: tool ?? \"none\"}",
            json!({"source": "microsoft-graph"}),
        );
        assert_eq!(s["k"], "microsoft-graph|x");
        assert_eq!(s["v"], "microsoft-graph");
        assert_eq!(s["t"], "none");
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
    fn base64_gives_the_utf8_bytes_of_a_text() {
        let p = one_emit(
            r#"emit "vx.t", {a: base64("a"), e: base64(word), n: base64(nothing), d: base64(doc), raw: unreadable.base64 ?? base64(unreadable.text)}"#,
            json!({"word": "é", "nothing": null, "doc": {"k": 1}, "unreadable": {"error": "x", "text": "id=42;bad", "base64": null}}),
        );
        assert_eq!(p["a"], "YQ==");
        assert_eq!(p["e"], "w6k=");
        assert_eq!(p["n"], Value::Null);
        assert_eq!(p["d"], "eyJrIjoxfQ==");
        // a message that is not JSON, in base64 whatever its bytes are
        assert_eq!(p["raw"], "aWQ9NDI7YmFk");
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
        // Adding a NEW key to an existing table is a data extension (N1): it
        // inserts span-exact and still parses/runs.
        let s4 = set_literal(&s2, "T", "bloquante", &json!("P1")).unwrap();
        assert!(s4.contains("\"bloquante\": \"P1\""));
        assert!(s4.contains("\"haute\": \"P9\"")); // existing entries intact
        eval_flow(&s4, json!({})).unwrap();
        // …but only a table can gain a key — a constant or a list cannot, and a
        // missing literal is still an error.
        assert!(set_literal(src, "N", "bloquante", &json!("x")).is_err());
        assert!(set_literal(src, "L", "bloquante", &json!("x")).is_err());
        assert!(set_literal(src, "NOPE", "-", &json!(1)).is_err());
    }

    /// A JSON escape spelled out: a backslash, a u, four hex digits.
    fn esc(hex: &str) -> String {
        format!("{}u{hex}", '\\')
    }

    #[test]
    fn an_error_in_an_fstring_says_the_line_it_is_on() {
        let e = parse("x = 1\ny = 2\nz = f\"{x ?? \"C:\\q\"}\"\n").err().unwrap();
        assert!(e.starts_with("line 3: unknown escape"), "{e}");
        let e = parse("x = 1\n\nz = f\"a {x +} b\"\n").err().unwrap();
        assert!(e.starts_with("line 3: "), "{e}");
        // what the f-string's {…} holds still runs
        assert_eq!(one_emit("a = 2\nz = f\"{a + 1}{{x}}\"\nemit \"vx.t\", {z: z}\n", json!({})), json!({"z": "3{x}"}));
    }

    #[test]
    fn a_table_gains_a_key_whatever_its_text_holds() {
        // an escape JSON does not read, bare keys, and a brace in a comment after it
        let src = "T = {\"a\": \"x\\{y\", b: \"}\"}  # rows like {a: b}\n";
        let out = set_literal(src, "T", "c", &json!("z")).unwrap();
        assert_eq!(get_literal(&out, "T", "-").unwrap(), json!({"a": "x{y", "b": "}", "c": "z"}));
        assert!(out.ends_with("# rows like {a: b}\n"), "{out}");
        let empty = set_literal("T = {}\n", "T", "k", &json!(1)).unwrap();
        assert_eq!(get_literal(&empty, "T", "-").unwrap(), json!({"k": 1}));
        assert!(set_literal("N = 5\n", "N", "k", &json!(1)).unwrap_err().contains("not a table"));
    }

    #[test]
    fn a_literal_set_live_reads_back_as_it_was_set() {
        // `set_literal` writes JSON: a CR, a control character, a quote and a
        // backslash come back as they were set, not as their letter.
        for v in ["a\r\nb", "\u{1}bell", "say \"hi\"", "back\\slash", "tab\there", "é😀"] {
            let src = set_literal("V = \"x\"\nemit \"vx.t\", {v: V}\n", "V", "-", &json!(v)).unwrap();
            assert_eq!(one_emit(&src, json!({}))["v"], v, "{src}");
        }
        // JSON's escapes written by hand, in a string and in an f-string.
        let src = format!(
            "emit \"vx.t\", {{a: \"{e}{pair}\\/\\b\\f\", b: f\"{e}\\{{x\\}}\"}}",
            e = esc("00e9"),
            pair = esc("d83d") + &esc("de00"),
        );
        let p = one_emit(&src, json!({}));
        assert_eq!(p["a"], "é😀/\u{8}\u{c}");
        assert_eq!(p["b"], "é{x}");
        // An escape that stands for nothing says how a backslash is written.
        let e = eval_flow("emit \"vx.t\", {v: \"C:\\qa\"}", json!({})).unwrap_err();
        assert!(e.contains("line 1") && e.contains("unknown escape \\q") && e.contains("\\\\"), "{e}");
        // A surrogate alone, or a \u without its four digits.
        for bad in [esc("d83d") + "x", esc("de00"), esc("12") + "\""] {
            let e = eval_flow(&format!("emit \"vx.t\", {{v: \"{bad}\"}}"), json!({})).unwrap_err();
            assert!(e.contains("\\u"), "{bad}: {e}");
        }
    }

    /// A system to call on 127.0.0.1. It answers each request with what
    /// `reply` gives for its URL (a status, a body, a delay in ms), and keeps
    /// what it was sent: the method and URL, then each header.
    fn system(reply: fn(&str) -> (u16, &'static str, u64)) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let url = format!("http://{}", server.server_addr().to_ip().unwrap());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let kept = seen.clone();
        std::thread::spawn(move || {
            for request in server.incoming_requests() {
                let mut sent = format!("{} {}", request.method(), request.url());
                for h in request.headers() {
                    sent.push_str(&format!("\n{}: {}", h.field, h.value));
                }
                kept.lock().unwrap().push(sent.to_ascii_lowercase());
                // each answer on a thread of its own: a slow one holds up no other
                std::thread::spawn(move || {
                    let (status, body, delay) = reply(request.url());
                    std::thread::sleep(std::time::Duration::from_millis(delay));
                    let json = "Content-Type: application/json".parse::<tiny_http::Header>().unwrap();
                    let answer = tiny_http::Response::from_string(body).with_status_code(status).with_header(json);
                    let _ = request.respond(answer);
                });
            }
        });
        (url, seen)
    }

    /// A project whose service `get_order` answers from `url`, with `extra`
    /// settings; its token is a secret.
    fn declared(name: &str, url: &str, extra: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("vejas-answers-{name}-{}", std::process::id()));
        std::fs::create_dir_all(root.join("services")).unwrap();
        let declaration = r#"driver "http-rpc"
BASE_URL = "@URL@"
URL = "{BASE_URL}/orders/{id}"
HEADERS = {Authorization: f"Bearer {secret("orders/token")}"}
"#
        .replace("@URL@", url)
            + extra;
        std::fs::write(root.join("services").join("get_order.vjs"), declaration).unwrap();
        root
    }

    fn live(root: PathBuf) -> Engine {
        let store = MapStore([("orders/token".to_string(), "s3cr3t-token".to_string())].into());
        Engine::with_secrets(root, "default".into(), Arc::new(store))
    }

    /// The error of a run that must fail.
    fn failed(run: Result<Ctx, String>) -> String {
        match run {
            Err(e) => e,
            Ok(ctx) => panic!("the run did not fail: {:?}", ctx.emits),
        }
    }

    #[test]
    fn a_flow_reads_what_a_service_answers() {
        let (url, seen) = system(|_| (200, r#"{"total": 42}"#, 0));
        let mut engine = live(declared("reads", &url, ""));
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        engine.carry(crate::trace::Context::parse(tp, None));
        let prog = parse(
            "o = invoke get_order(id: event.id)\nemit \"vx.t\", {status: o.status, total: o.body.total, kind: o.headers[\"content-type\"]}\n",
        )
        .unwrap();
        let ctx = run(&prog, &json!({"id": "A/1"}), &mut engine).unwrap();
        assert_eq!(ctx.emits[0].1, json!({"status": 200, "total": 42, "kind": "application/json"}));
        let sent = seen.lock().unwrap()[0].clone();
        assert!(sent.starts_with("get /orders/a%2f1\n"), "{sent}");
        assert!(sent.contains("\nauthorization: bearer s3cr3t-token"), "{sent}");
        assert!(sent.contains("\ntraceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-"), "{sent}");
        assert_eq!(engine.service_makes("get_order").unwrap(), ["status", "body", "headers"]);
    }

    #[test]
    fn a_service_that_gives_no_answer_fails_the_run_and_says_what_comes_next() {
        let (url, _) = system(|path| match path {
            p if p.ends_with("/down") => (503, r#"{"error":"down"}"#, 0),
            p if p.ends_with("/gone") => (404, "", 0),
            p if p.ends_with("/slow") => (200, "{}", 600),
            _ => (200, "{}", 0),
        });
        let prog = parse("o = invoke get_order(id: event.id)\nemit \"vx.t\", {s: o.status}\n").unwrap();
        let mut engine = live(declared("fails", &url, "ATTEMPTS = 2\nDELAY_MS = 250\nTIMEOUT_MS = 200\n"));
        let e = failed(run(&prog, &json!({"id": "down"}), &mut engine));
        assert_eq!(e, "get_order: HTTP 503: {\"error\":\"down\"}");
        let f = engine.take_failure().unwrap();
        assert!(f.retry && !f.timed_out, "{f:?}");
        assert_eq!((f.attempts, f.delay), (2, std::time::Duration::from_millis(250)));
        assert_eq!(failed(run(&prog, &json!({"id": "gone"}), &mut engine)), "get_order: HTTP 404");
        assert!(!engine.take_failure().unwrap().retry, "a 404 is not tried again");
        assert_eq!(failed(run(&prog, &json!({"id": "slow"}), &mut engine)), "get_order: timeout after 200 ms");
        let f = engine.take_failure().unwrap();
        assert!(f.retry && f.timed_out, "{f:?}");
        // A run that gets its answer leaves no failure behind.
        run(&prog, &json!({"id": "ok"}), &mut engine).unwrap();
        assert!(engine.take_failure().is_none());
        // A status the service lists is an answer the flow decides on.
        let mut engine = live(declared("answers", &url, "ANSWERS = [200, 404]\n"));
        assert_eq!(run(&prog, &json!({"id": "gone"}), &mut engine).unwrap().emits[0].1, json!({"s": 404}));
        // Nothing listens: a later delivery may get through.
        let mut engine = live(declared("network", "http://127.0.0.1:9", ""));
        let e = failed(run(&prog, &json!({"id": "x"}), &mut engine));
        assert!(e.starts_with("get_order: network: "), "{e}");
        assert!(engine.take_failure().unwrap().retry);
        // An argument the URL does not take is an error, not a request without it.
        let typo = parse("o = invoke get_order(id: 1, idd: 2)\n").unwrap();
        let e = failed(run(&typo, &json!({}), &mut live(declared("typo", &url, ""))));
        assert!(e.contains("idd appears nowhere"), "{e}");
    }

    #[test]
    fn a_run_that_is_shown_calls_no_system() {
        let (url, seen) = system(|_| (200, "{}", 0));
        let root = declared("shown", &url, "");
        std::fs::write(root.join("services").join("post_it.vjs"), "driver \"http-out\"\nURL = \"http://x\"\n").unwrap();
        let prog = parse("o = invoke get_order(id: event.id)\nemit \"vx.t\", {s: o.status}\n").unwrap();
        let mut engine = Engine::shadow(root.clone(), "default".into());
        let e = failed(run(&prog, &json!({"id": "A1"}), &mut engine));
        assert!(e.starts_with("get_order(id: \"A1\") answers from a system") && e.contains("calls none"), "{e}");
        assert!(seen.lock().unwrap().is_empty(), "the system was called");
        // the run names the call it could not make, so a comparison leaves it out
        assert_eq!(engine.take_unrecorded().as_deref(), Some("get_order(id: \"A1\")"));
        assert!(!engine.made_a_call());
        // an argument the service does not take is what a shown run reports
        let typo = parse("o = invoke get_order(idd: event.id)\n").unwrap();
        let e = failed(run(&typo, &json!({"id": "A1"}), &mut engine));
        assert_eq!(e, "get_order: the argument idd appears nowhere in its URL");
        assert!(engine.take_unrecorded().is_none());
        // Only a driver that answers can stand behind a service.
        let e = failed(run(&parse("o = invoke post_it(a: 1)\n").unwrap(), &json!({}), &mut live(root)));
        assert!(e.contains("declares driver \"http-out\", which answers no call"), "{e}");
    }

    #[test]
    fn what_a_flow_passes_comes_back_with_the_answer() {
        let (url, _) = system(|_| (200, r#"{"total": 42}"#, 0));
        let mut engine = live(declared("merge", &url, ""));
        let prog = parse("invoke get_order(id: event.ref)\nemit \"vx.t\", {id: id, s: status, total: body.total}\n").unwrap();
        let ctx = run(&prog, &json!({"ref": "X1"}), &mut engine).unwrap();
        assert_eq!(ctx.emits[0].1, json!({"id": "X1", "s": 200, "total": 42}));
        assert!(engine.made_a_call());
    }

    #[test]
    fn a_call_follows_no_redirect() {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let url = format!("http://{}", server.server_addr().to_ip().unwrap());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let kept = seen.clone();
        let elsewhere = format!("Location: {url}/elsewhere").parse::<tiny_http::Header>().unwrap();
        std::thread::spawn(move || {
            for request in server.incoming_requests() {
                kept.lock().unwrap().push(request.url().to_string());
                let _ = request.respond(tiny_http::Response::empty(302).with_header(elsewhere.clone()));
            }
        });
        let prog = parse("o = invoke get_order(id: event.id)\n").unwrap();
        let mut engine = live(declared("redirect", &url, ""));
        assert_eq!(failed(run(&prog, &json!({"id": "A1"}), &mut engine)), "get_order: HTTP 302");
        assert!(!engine.take_failure().unwrap().retry);
        assert_eq!(*seen.lock().unwrap(), ["/orders/A1"]);
    }

    #[test]
    fn a_call_error_shows_no_secret_its_system_echoes() {
        crate::secrets::hand_out("s3cr3t-token");
        let (url, _) = system(|_| (401, r#"{"error": "bad token s3cr3t-token"}"#, 0));
        let prog = parse("o = invoke get_order(id: event.id)\n").unwrap();
        let e = failed(run(&prog, &json!({"id": "A1"}), &mut live(declared("echo", &url, ""))));
        assert!(e.starts_with("get_order: HTTP 401") && !e.contains("s3cr3t-token"), "{e}");
    }

    #[test]
    fn a_declaration_holds_settings_and_a_secret_its_store_lacks_is_not_tried_again() {
        let (url, seen) = system(|_| (200, "{}", 0));
        let root = declared("settings", &url, "");
        let services = root.join("services");
        std::fs::write(services.join("loops.vjs"), "driver \"http-rpc\"\nURL = \"http://x/{id}\"\nX = invoke loops(id: 1)\n").unwrap();
        std::fs::write(
            services.join("locked.vjs"),
            format!("driver \"http-rpc\"\nURL = \"{url}/{{id}}\"\nHEADERS = {{Authorization: secret(\"missing/key\")}}\n"),
        )
        .unwrap();
        // a declaration that invokes is refused, never run
        let mut engine = live(root.clone());
        let e = failed(run(&parse("o = invoke loops(id: 1)\n").unwrap(), &json!({}), &mut engine));
        assert!(e.contains("its file holds settings, and invokes and emits nothing"), "{e}");
        // a secret the store says it does not have: no delivery gives it (a
        // store that does not answer: what_the_runtime_did_not_give_...)
        let e = failed(run(&parse("o = invoke locked(id: 1)\n").unwrap(), &json!({}), &mut engine));
        assert!(e.contains("missing/key") && e.contains("not found"), "{e}");
        let f = engine.take_failure().unwrap();
        assert!(!f.retry && !f.timed_out && f.service == "locked", "{f:?}");
        assert!(seen.lock().unwrap().is_empty());
    }

    #[test]
    fn a_check_reads_what_a_flow_calls_without_running_it() {
        let (url, _) = system(|_| (200, "{}", 0));
        let root = declared("check", &url, "");
        let services = root.join("services");
        std::fs::write(services.join("tag.vjs"), "t = 1\n").unwrap();
        std::fs::write(services.join("enrich.vjs"), "o = invoke get_order(id: 1)\n").unwrap();
        let mut engine = Engine::shadow(root.clone(), "default".into());
        let flow = parse("o = invoke get_order(idd: 1)\np = invoke get_order(id: 1, BASE_URL: \"h\")\ninvoke tag(x: 1)\n").unwrap();
        assert_eq!(
            engine.check_invokes(&flow),
            [
                "invoke get_order: the argument idd appears nowhere in its URL",
                "invoke get_order: the argument BASE_URL has the name of a setting of the service",
            ]
        );
        assert!(engine.reaches_system(&flow));
        assert!(!engine.reaches_system(&parse("invoke tag(x: 1)\n").unwrap()));
        assert!(engine.reaches_system(&parse("invoke enrich(x: 1)\n").unwrap()), "through a service of code");
        assert!(engine.check_invokes(&parse("o = invoke get_order(id: 1)\n").unwrap()).is_empty());
    }

    #[test]
    fn an_engine_picks_up_a_service_edit_once_its_cache_is_old_enough() {
        let root = std::env::temp_dir().join(format!("vejas-refresh-{}", std::process::id()));
        std::fs::create_dir_all(root.join("services")).unwrap();
        std::fs::write(root.join("services").join("tag.vjs"), "t = 1\n").unwrap();
        let prog = parse("invoke tag(x: 1)\nemit \"vx.t\", {t: t}\n").unwrap();
        let mut engine = Engine::live(root.clone(), "default".into());
        let t = |engine: &mut Engine| run(&prog, &json!({}), engine).unwrap().emits[0].1["t"].clone();
        assert_eq!(t(&mut engine), 1);
        std::fs::write(root.join("services").join("tag.vjs"), "t = 2\n").unwrap();
        engine.refresh(std::time::Duration::from_secs(3600));
        assert_eq!(t(&mut engine), 1, "loaded less than an hour ago");
        engine.refresh(std::time::Duration::ZERO);
        assert_eq!(t(&mut engine), 2);
    }

    struct MapStore(std::collections::HashMap<String, String>);
    impl crate::secrets::SecretStore for MapStore {
        fn kind(&self) -> &'static str { "map" }
        fn get(&self, r: &str) -> Result<String, String> {
            self.0.get(r).cloned().ok_or_else(|| format!("secret {r:?} not found"))
        }
    }

    #[test]
    fn an_emit_subject_that_cannot_go_on_the_wire_fails_the_run() {
        let prog = parse("emit f\"vx.{t}\", {a: 1}\n").unwrap();
        let run_with = |t: &str| {
            let mut engine = Engine::live(std::env::temp_dir(), "default".into());
            run(&prog, &json!({"t": t}), &mut engine).map(|ctx| ctx.emits.len())
        };
        assert_eq!(run_with("orders.eu-west_1"), Ok(1));
        for bad in ["a b", "a\tb", "a\nb", "a\r\nPUB b 0", "a\u{0}b", "a\u{a0}b"] {
            let err = run_with(bad).unwrap_err();
            assert!(err.contains("not a valid subject"), "{bad:?}: {err}");
        }
        assert!(!valid_subject(""));
    }

    #[test]
    fn shadow_never_sees_a_secret_value() {
        let mut map = std::collections::HashMap::new();
        map.insert("a/b".to_string(), "s3cret".to_string());
        let store = std::sync::Arc::new(MapStore(map));
        let shadow = std::sync::Arc::new(crate::secrets::RedactedStore::new(store));
        let prog = parse("v = secret(\"a/b\")\nemit \"o\", {v: v, up: upper(v)}\n").unwrap();
        let mut engine = Engine::with_secrets(std::env::temp_dir(), "default".into(), shadow.clone());
        let ctx = run(&prog, &json!({}), &mut engine).unwrap();
        let shown = serde_json::to_string(&ctx.emits).unwrap() + &format!("{:?}", ctx.vars);
        assert!(!shown.contains("s3cret") && !shown.contains("S3CRET"), "{shown}");
        assert!(shown.contains("<secret:a/b>"), "{shown}");
        // fail-closed like a real run: a missing secret still aborts
        let prog = parse("v = secret(\"missing/x\")\n").unwrap();
        let mut engine = Engine::with_secrets(std::env::temp_dir(), "default".into(), shadow);
        assert!(run(&prog, &json!({}), &mut engine).is_err());
    }

    #[test]
    fn secret_resolves_and_fails_closed() {
        let mut m = std::collections::HashMap::new();
        m.insert("api/key".to_string(), "TOP-SECRET".to_string());
        let store = std::sync::Arc::new(MapStore(m));
        let prog = parse("k = secret(\"api/key\")\nemit \"vx.t\", {len: len(k), auth: len(k) > 0}").unwrap();
        // static reference is captured for the panel, value is not
        assert_eq!(prog.secret_refs, vec![("k".to_string(), "api/key".to_string())]);
        // NESTED references are captured too (a HEADERS doc with an f-string)
        let nested = parse(
            "HEADERS = {\"Authorization\": f\"Bearer {secret(\"reglyze/api_token\")}\"}\n",
        )
        .unwrap();
        assert_eq!(
            nested.secret_refs,
            vec![("HEADERS".to_string(), "reglyze/api_token".to_string())]
        );
        let mut eng = Engine::with_secrets(std::env::temp_dir(), "default".into(), store.clone());
        let ctx = run(&prog, &json!({}), &mut eng).unwrap();
        assert_eq!(ctx.emits[0].1, json!({"len": 10, "auth": true}));
        // missing secret fails closed
        let bad = parse("k = secret(\"nope/x\")\nemit \"vx.t\", {k: k}").unwrap();
        let mut eng2 = Engine::with_secrets(std::env::temp_dir(), "default".into(), store);
        match run(&bad, &json!({}), &mut eng2) {
            Err(e) => assert!(e.contains("not found")),
            Ok(_) => panic!("expected fail-closed on missing secret"),
        }
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

        let mut engine = Engine::live(root.clone(), "default".into());
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
        let mut engine = Engine::live(root.clone(), "default".into());
        // outer is exported; inner is private but called FROM WITHIN pkg n -> allowed
        let prog = parse("invoke n:outer(v: 9)\n").unwrap();
        let ctx = run(&prog, &json!({}), &mut engine).unwrap();
        assert_eq!(ctx.emits, vec![("vx.deep".to_string(), json!({"r": 10}))]);
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A run's steps, written short: `3` a line, `3>vx.s` an emit, `3<201` a
    /// respond, `3 caught` a failure a catch took, `enter p:s` a service
    /// starting, `p:s:2` a line in it, `...n` the steps left out.
    fn steps_of(engine: &mut Engine) -> Vec<String> {
        let steps = engine.take_steps().expect("the engine records steps");
        let one = |st: &Step| {
            let at = st.service.as_deref().map(|s| format!("{s}:")).unwrap_or_default();
            match &st.what {
                What::Line(l) => format!("{at}{l}"),
                What::Emit(l, subject) => format!("{at}{l}>{subject}"),
                What::Respond(l, status) => format!("{at}{l}<{status}"),
                What::Enter(_) => format!("enter {}", st.service.as_deref().unwrap_or("?")),
                What::Caught(l) => format!("{at}{l} caught"),
            }
        };
        let mut out: Vec<String> = steps.head().iter().map(one).collect();
        if steps.skipped() > 0 {
            out.push(format!("...{}", steps.skipped()));
        }
        out.extend(steps.tail().map(one));
        // `!`: the run failed on this statement
        if let (true, Some(last)) = (steps.failed(), out.last_mut()) {
            last.push('!');
        }
        out
    }

    #[test]
    fn steps_are_the_lines_a_run_went_through() {
        let src = [
            "LIMIT = 100",
            "# a comment",
            "total = amount * 2",
            "if total > LIMIT:",
            "  emit \"vx.big\", {t: total}",
            "elif total > 10:",
            "  emit \"vx.mid\", {t: total}",
            "else:",
            "  x = 1",
            "end",
            "for i in items:",
            "  n = i",
            "end",
            "doc = {",
            "  a: 1,",
            "  b: 2",
            "}",
            "",
            "respond 201, doc",
        ]
        .join("\n");
        let prog = parse(&src).unwrap();
        let mut engine = Engine::live(std::env::temp_dir(), "default".into());
        engine.record_steps(true);
        run(&prog, &json!({"amount": 30, "items": [1, 2]}), &mut engine).unwrap();
        // the surface constant on line 1 runs the same way every time: left out
        assert_eq!(steps_of(&mut engine), ["3", "4", "7>vx.mid", "11", "12", "12", "14", "19<201"]);
        // nothing is recorded unless asked
        engine.record_steps(false);
        run(&prog, &json!({"amount": 30, "items": []}), &mut engine).unwrap();
        assert!(engine.take_steps().is_none());
    }

    #[test]
    fn steps_name_the_service_a_line_is_in_and_its_version() {
        let root = std::env::temp_dir().join(format!("vejas-steps-{}", std::process::id()));
        let pkg = root.join("packages").join("billing");
        std::fs::create_dir_all(root.join("services")).unwrap();
        std::fs::create_dir_all(pkg.join("services")).unwrap();
        let double = "twice = n * 2\nhalf = n / 2\n";
        std::fs::write(root.join("services").join("double.vjs"), double).unwrap();
        std::fs::write(pkg.join("services").join("fmt.vjs"), "invoke inner(v: x)\nlabel = f\"<{r}>\"\n").unwrap();
        std::fs::write(pkg.join("services").join("inner.vjs"), "r = v\n").unwrap();
        std::fs::write(pkg.join("package.vjs"), "EXPORTS = [\"fmt\"]\n").unwrap();
        let prog = parse("invoke double(n: 4)\nd = invoke billing:fmt(x: twice)\nemit \"vx.t\", {l: d.label}\n").unwrap();
        let mut engine = Engine::live(root.clone(), "default".into());
        engine.record_steps(true);
        let ctx = run(&prog, &json!({}), &mut engine).unwrap();
        assert_eq!(ctx.emits[0].1["l"], "<8>");
        engine.record_steps(true);
        run(&prog, &json!({}), &mut engine).unwrap();
        let steps = engine.take_steps().unwrap();
        let version = steps.head().iter().find_map(|st| match &st.what {
            What::Enter(v) if st.service.as_deref() == Some("default:double") => Some(v.to_string()),
            _ => None,
        });
        assert_eq!(version, Some(crate::versions::hash_content(double)));
        // each service once, with its version: a reader finds a service's
        // code by it even when the step where it started was left out
        let services: Vec<(String, String)> =
            steps.services().iter().map(|(name, version)| (name.to_string(), version.to_string())).collect();
        assert_eq!(services.len(), 3, "{services:?}");
        assert_eq!(services[0], ("default:double".to_string(), crate::versions::hash_content(double)));
        assert_eq!(services[1].0, "billing:fmt");
        assert_eq!(services[2].0, "billing:inner");
        // a run that records no steps never hashes a service
        let mut quiet = Engine::live(root.clone(), "default".into());
        run(&prog, &json!({}), &mut quiet).unwrap();
        assert!(quiet.cache.values().all(|service| service.version.get().is_none()));
        engine.record_steps(true);
        run(&prog, &json!({}), &mut engine).unwrap();
        assert_eq!(
            steps_of(&mut engine),
            [
                "1",
                "enter default:double",
                "default:double:1",
                "default:double:2",
                "2",
                "enter billing:fmt",
                "billing:fmt:1",
                "enter billing:inner",
                "billing:inner:1",
                "billing:fmt:2",
                "3>vx.t",
            ]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_long_run_keeps_its_first_and_last_steps() {
        let prog = parse("for i in items:\n  n = i\nend\nemit \"vx.done\", {n: n}\n").unwrap();
        let items: Vec<i32> = (0..1000).collect();
        let mut engine = Engine::live(std::env::temp_dir(), "default".into());
        engine.record_steps(true);
        run(&prog, &json!({"items": items}), &mut engine).unwrap();
        let steps = steps_of(&mut engine);
        // the `for`, a thousand times its body, the emit
        assert_eq!(steps.len(), STEPS_HEAD + 1 + STEPS_TAIL);
        assert_eq!(steps[..3], ["1", "2", "2"]);
        assert_eq!(steps[STEPS_HEAD], format!("...{}", 1002 - STEPS_HEAD - STEPS_TAIL));
        assert_eq!(steps.last().unwrap(), "4>vx.done");
    }

    #[test]
    fn a_failed_run_ends_on_the_statement_that_failed() {
        let prog = parse("a = 1\nfor i in items:\n  emit f\"vx.{i}\", {i: i}\nend\nb = 2\n").unwrap();
        let mut engine = Engine::live(std::env::temp_dir(), "default".into());
        engine.record_steps(true);
        let Err(err) = run(&prog, &json!({"items": ["ok", "not ok"]}), &mut engine) else {
            panic!("a subject with a space must fail the run");
        };
        assert!(err.contains("not a valid subject"), "{err}");
        assert_eq!(steps_of(&mut engine), ["1", "2", "3>vx.ok", "3!"]);

        let root = std::env::temp_dir().join(format!("vejas-failed-{}", std::process::id()));
        std::fs::create_dir_all(root.join("services")).unwrap();
        std::fs::write(root.join("services").join("route.vjs"), "o = 1\nsubject = \"vx.bad subject\"\n").unwrap();
        std::fs::write(root.join("services").join("broken.vjs"), "emit \"vx.bad subject\", {}\n").unwrap();
        let mut engine = Engine::live(root.clone(), "default".into());
        let mut failed = |src: &str| {
            engine.record_steps(true);
            assert!(run(&parse(src).unwrap(), &json!({"a": true}), &mut engine).is_err(), "{src}");
            steps_of(&mut engine)
        };
        // the statement fails once an invoke in it has returned: the service's
        // steps came after its own, so it says it again
        assert_eq!(
            failed("a = 1\nemit (invoke route(o: 1)).subject, {a: a}\nb = 2\n"),
            ["1", "2", "enter default:route", "default:route:1", "default:route:2", "2!"]
        );
        // the service's statement failed: the run ends there, and the
        // statement that invoked it does not say it again
        assert_eq!(failed("r = invoke broken()\n"), ["1", "enter default:broken", "default:broken:1!"]);
        // nor does the block around a statement that failed
        assert_eq!(failed("if a:\n  emit \"vx.bad subject\", {}\nend\n"), ["1", "2!"]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_respond_in_a_service_answers_no_one() {
        let root = std::env::temp_dir().join(format!("vejas-respond-{}", std::process::id()));
        std::fs::create_dir_all(root.join("services")).unwrap();
        std::fs::write(root.join("services").join("auth.vjs"), "ok = false\nrespond 401, {error: \"no\"}\n").unwrap();
        let prog = parse("invoke auth(token: t)\nrespond 201, {ok: ok}\n").unwrap();
        let mut engine = Engine::live(root.clone(), "default".into());
        engine.record_steps(true);
        let ctx = run(&prog, &json!({"t": "x"}), &mut engine).unwrap();
        assert_eq!(ctx.response.map(|(status, _)| status), Some(json!(201)));
        // run_service drops the service's response: its line is a line, no more
        assert_eq!(
            steps_of(&mut engine),
            ["1", "enter default:auth", "default:auth:1", "default:auth:2", "2<201"]
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn a_byte_order_mark_is_not_code_and_a_wide_character_is_an_error() {
        let src = format!("{}x = 1\nemit \"vx.a\", {{x: x}}\n", '\u{feff}');
        let prog = parse(&src).unwrap();
        let mut engine = Engine::live(std::env::temp_dir(), "default".into());
        engine.record_steps(true);
        let ctx = run(&prog, &json!({}), &mut engine).unwrap();
        assert_eq!(ctx.emits, vec![("vx.a".to_string(), json!({"x": 1}))]);
        assert_eq!(steps_of(&mut engine), ["1", "2>vx.a"]);
        // a character of three bytes outside a string once panicked the lexer
        for (src, found) in [("x = 1 \u{2014} 2\n", '\u{2014}'), ("y = 2\n\u{feff}z = 3\n", '\u{feff}')] {
            let err = parse(src).err().expect("not code");
            assert!(err.contains(&format!("unexpected character {found:?}")), "{err}");
        }
    }

    #[test]
    fn a_buffer_handed_back_comes_back_empty() {
        let mut steps = Steps::new();
        for i in 0..40 {
            steps.push(Step { service: None, what: What::Emit(i, format!("vx.{i}")) });
        }
        drop(steps);
        let again = Steps::new();
        assert!(again.is_empty() && again.head().is_empty() && again.services().is_empty());
    }

    #[test]
    fn a_step_is_found_again_while_it_is_kept() {
        let mut steps = Steps::new();
        let n = STEPS_HEAD + 2 * STEPS_TAIL;
        for i in 0..n {
            steps.push(Step { service: None, what: What::Line(i as u32) });
        }
        let line = |steps: &mut Steps, k: usize| {
            steps.get_mut(k as u64).map(|st| match st.what {
                What::Line(l) => l as usize,
                _ => unreachable!(),
            })
        };
        assert_eq!(line(&mut steps, 0), Some(0));
        assert_eq!(line(&mut steps, STEPS_HEAD - 1), Some(STEPS_HEAD - 1));
        assert_eq!(line(&mut steps, STEPS_HEAD), None);
        assert_eq!(line(&mut steps, n - STEPS_TAIL - 1), None);
        assert_eq!(line(&mut steps, n - STEPS_TAIL), Some(n - STEPS_TAIL));
        assert_eq!(line(&mut steps, n - 1), Some(n - 1));
        assert_eq!(line(&mut steps, n), None);
        assert_eq!((steps.len(), steps.skipped()), (STEPS_HEAD + STEPS_TAIL, STEPS_TAIL as u64));
    }

    #[test]
    fn try_catch_and_remember_parse_and_leave_their_names_free() {
        let prog = parse("try:\n    o = invoke get_order(id: 1)\ncatch e:\n    emit \"vx.r\", {why: e.error}\nend\n").unwrap();
        assert!(matches!(&prog.stmts[0], Stmt::Try(body, Some(name), handler) if body.len() == 1 && name == "e" && handler.len() == 1));
        assert!(matches!(&parse("try:\n    x = 1\ncatch:\n    x = 2\nend\n").unwrap().stmts[0], Stmt::Try(_, None, _)));
        parse("WINDOW = \"7d\"\nremember event.id for WINDOW\nremember event.id for \"36h\"\n").unwrap();
        // a constant may be written after the statement that names it
        parse("remember event.id for WINDOW\nWINDOW = \"90s\"\n").unwrap();
        let error = |src: &str| parse(src).err().unwrap_or_else(|| panic!("{src:?} parsed"));
        let e = error("try:\n    x = 1\nend\n");
        assert!(e.contains("expected `catch:` or `catch e:`"), "{e}");
        let e = error("x = 1\ncatch e:\n    y = 2\nend\n");
        assert!(e.contains("`catch` without `try`"), "{e}");
        let e = error("try:\n    if x:\n        y = 1\n    catch e:\n        y = 2\nend\n");
        assert!(e.contains("expected elif/else/end"), "{e}");
        let e = error("remember event.id\n");
        assert!(e.contains("expected for and a window"), "{e}");
        let e = error("remember event.id for \"7 days\"\n");
        assert!(e.contains("\"7 days\" is no window"), "{e}");
        let e = error("remember event.id for window\n");
        assert!(e.contains("a duration the flow writes"), "{e}");
        let e = error("remember event.id for WINDOW\n");
        assert!(e.contains("WINDOW is no constant of the flow"), "{e}");
        let e = error("WINDOW = 7\nremember event.id for WINDOW\n");
        assert!(e.contains("WINDOW is no constant of the flow"), "{e}");
        let e = error("WINDOW = \"0d\"\nremember event.id for WINDOW\n");
        assert!(e.contains("WINDOW: \"0d\" is no window"), "{e}");
        let e = error("remember event.id for \"3651d\"\n");
        assert!(e.contains("longer than the longest window"), "{e}");
        assert_eq!(window_of("90s"), Ok(std::time::Duration::from_secs(90)));
        assert_eq!(window_of(" 24h "), Ok(std::time::Duration::from_secs(86_400)));
        assert!(window_of("7").is_err() && window_of("d").is_err() && window_of("-1d").is_err() && window_of("1.5h").is_err());
        // a variable, a field or a key keeps such a name
        let prog = parse("try = event.try\nremember = catch + 1\nemit \"vx.t\", {try: try, catch: remember}\n").unwrap();
        let ctx = run(&prog, &json!({"try": 1, "catch": 2}), &mut Engine::live(std::env::temp_dir(), "default".into())).unwrap();
        assert_eq!(ctx.emits[0].1, json!({"try": 1, "catch": 3}));
    }

    #[test]
    fn a_catch_takes_a_failure_and_drops_what_its_try_did() {
        let prog = parse(
            r#"emit "vx.before", {n: 1}
try:
    emit "vx.inside", {n: 2}
    remember "k1" for "1d"
    respond 201, {ok: true}
    x = event.n.deep
catch e:
    emit "vx.caught", {error: e.error, service: e.service, status: e.status, attempts: e.attempts}
end
emit "vx.after", {x: x}
"#,
        )
        .unwrap();
        let subjects = |ctx: &Ctx| ctx.emits.iter().map(|(s, _)| s.clone()).collect::<Vec<_>>();
        let mut engine = Engine::live(std::env::temp_dir(), "default".into());
        let ctx = run(&prog, &json!({"n": 5}), &mut engine).unwrap();
        assert_eq!(subjects(&ctx), ["vx.before", "vx.caught", "vx.after"]);
        assert_eq!(
            ctx.emits[1].1,
            json!({"error": "cannot read field \"deep\" of 5", "service": null, "status": null, "attempts": 1})
        );
        assert!(ctx.marks.is_empty() && ctx.response.is_none(), "what the try did before its failure is dropped");
        // a try that succeeds keeps what it did, and its catch does not run
        let ctx = run(&prog, &json!({"n": {"deep": 7}}), &mut engine).unwrap();
        assert_eq!(subjects(&ctx), ["vx.before", "vx.inside", "vx.after"]);
        assert_eq!(ctx.marks, [("k1".to_string(), std::time::Duration::from_secs(86_400))]);
        assert_eq!(ctx.response.as_ref().map(|(s, _)| s.clone()), Some(json!(201)));
        assert_eq!(ctx.emits[2].1, json!({"x": 7}));
        // a failure in a catch goes to the try around it, with what the inner catch sent
        let nested = parse(
            r#"try:
    try:
        a = event.n.deep
    catch inner:
        emit "vx.inner", {e: inner.error}
        b = event.n.deeper
    end
catch outer:
    emit "vx.outer", {e: outer.error}
end
"#,
        )
        .unwrap();
        let ctx = run(&nested, &json!({"n": 5}), &mut engine).unwrap();
        assert_eq!(subjects(&ctx), ["vx.outer"]);
        assert_eq!(ctx.emits[0].1["e"], "cannot read field \"deeper\" of 5");
        // and one in a catch no try holds fails the run
        let bare = parse("try:\n    a = event.n.deep\ncatch e:\n    b = e.error.x\nend\n").unwrap();
        let e = failed(run(&bare, &json!({"n": 5}), &mut engine));
        assert!(e.starts_with("cannot read field \"x\""), "{e}");
        // the name of the failure is the flow's, not a field it reads from its event
        let reads = event_reads(&parse("try:\n    o = invoke get_order(id: id)\ncatch e:\n    emit \"vx.r\", {why: e.error}\nend\n").unwrap());
        assert_eq!(reads, ["id"]);
    }

    #[test]
    fn a_catch_waits_for_the_delivery_that_spends_a_calls_attempts() {
        let (url, calls) = system(|path| match path {
            p if p.ends_with("/down") => (503, r#"{"error":"down"}"#, 0),
            p if p.ends_with("/gone") => (404, "", 0),
            _ => (200, "{}", 0),
        });
        let prog = parse(
            r#"try:
    o = invoke get_order(id: event.id)
    emit "vx.ok", {s: o.status}
catch e:
    code = e.status ?? "NETWORK"
    emit "vx.rejected", {code: f"API_HTTP_{code}", service: e.service, attempts: e.attempts}
end
"#,
        )
        .unwrap();
        let root = declared("catch", &url, "ATTEMPTS = 3\nWHEN_DOWN = \"dead-letter\"\n");
        let mut engine = live(root.clone());
        // a call a later delivery may answer fails the run: the message comes back
        for delivery in [1, 2] {
            engine.delivered(Some(delivery));
            let e = failed(run(&prog, &json!({"id": "down"}), &mut engine));
            assert!(e.starts_with("get_order: HTTP 503"), "{e}");
            assert!(engine.take_failure().is_some_and(|f| f.retry && f.status == Some(503)));
        }
        // on the delivery that spends its attempts, the catch takes it
        engine.delivered(Some(3));
        let ctx = run(&prog, &json!({"id": "down"}), &mut engine).unwrap();
        assert_eq!(
            ctx.emits,
            [("vx.rejected".to_string(), json!({"code": "API_HTTP_503", "service": "get_order", "attempts": 3}))]
        );
        assert!(engine.take_failure().is_none(), "a failure a catch took fails no run");
        // one the service does not try again, at once
        engine.delivered(Some(1));
        let ctx = run(&prog, &json!({"id": "gone"}), &mut engine).unwrap();
        assert_eq!(ctx.emits[0].1["code"], "API_HTTP_404");
        // a request has no later delivery: at once
        engine.delivered(None);
        let ctx = run(&prog, &json!({"id": "down"}), &mut engine).unwrap();
        assert_eq!(ctx.emits[0].1, json!({"code": "API_HTTP_503", "service": "get_order", "attempts": 1}));
        // on the last delivery the runtime makes, whatever ATTEMPTS says
        let mut many = live(declared("catch-many", &url, "ATTEMPTS = 50\nWHEN_DOWN = \"dead-letter\"\n"));
        many.delivered(Some(vejas_connector::MAX_DELIVERIES - 1));
        assert!(failed(run(&prog, &json!({"id": "down"}), &mut many)).starts_with("get_order: HTTP 503"));
        many.delivered(Some(vejas_connector::MAX_DELIVERIES));
        assert_eq!(run(&prog, &json!({"id": "down"}), &mut many).unwrap().emits[0].0, "vx.rejected");
        // a network error has no status
        let mut nowhere = live(declared("catch-network", "http://127.0.0.1:9", "ATTEMPTS = 1\nWHEN_DOWN = \"dead-letter\"\n"));
        nowhere.delivered(Some(1));
        let ctx = run(&prog, &json!({"id": "x"}), &mut nowhere).unwrap();
        assert_eq!(ctx.emits[0].1["code"], "API_HTTP_NETWORK");
        // a run that is shown makes no call, and its catch takes none it could not make
        let made = calls.lock().unwrap().len();
        let mut shown = Engine::shadow(root, "default".into());
        let e = failed(run(&prog, &json!({"id": "down"}), &mut shown));
        assert!(e.contains("calls none"), "{e}");
        assert!(shown.take_unrecorded().is_some());
        assert_eq!(calls.lock().unwrap().len(), made, "a shown run called the system");
    }

    #[test]
    fn a_catch_waits_while_its_flow_holds_the_message_until_a_release() {
        let (url, _calls) = system(|path| match path {
            p if p.ends_with("/down") => (503, r#"{"error":"down"}"#, 0),
            p if p.ends_with("/gone") => (404, "", 0),
            _ => (200, "{}", 0),
        });
        let prog = parse(
            r#"try:
    o = invoke get_order(id: event.id)
    emit "vx.ok", {s: o.status}
catch e:
    emit "vx.rejected", {status: e.status}
end
"#,
        )
        .unwrap();
        // a declared service holds by default (ADR-0040)
        let root = declared("hold", &url, "ATTEMPTS = 2\n");
        let mut engine = live(root);
        assert!(engine.holds_its_source(&prog));
        for delivery in [1, 2, 9] {
            engine.delivered(Some(delivery));
            let e = failed(run(&prog, &json!({"id": "down"}), &mut engine));
            assert!(e.starts_with("get_order: HTTP 503"), "{e}");
            let f = engine.take_failure().expect("the failure the hold reads");
            assert!(f.holds(), "{f:?}");
        }
        // released, its catch takes it now
        engine.releasing(true);
        let ctx = run(&prog, &json!({"id": "down"}), &mut engine).unwrap();
        assert_eq!(ctx.emits[0].0, "vx.rejected");
        engine.releasing(false);
        // a failure no later try changes is caught at once
        engine.delivered(Some(1));
        let ctx = run(&prog, &json!({"id": "gone"}), &mut engine).unwrap();
        assert_eq!(ctx.emits[0].1, json!({"status": 404}));
        // a request is never held: its catch takes it at once
        engine.delivered(None);
        assert_eq!(run(&prog, &json!({"id": "down"}), &mut engine).unwrap().emits[0].0, "vx.rejected");
        // a service that says dead-letter keeps today's path
        let mut dl = live(declared("hold-dl", &url, "WHEN_DOWN = \"dead-letter\"\n"));
        assert!(dl.reaches_system(&prog) && !dl.holds_its_source(&prog));
        dl.delivered(Some(1));
        failed(run(&prog, &json!({"id": "down"}), &mut dl));
        assert!(dl.take_failure().is_some_and(|f| f.retry && !f.holds()));
        // a WHEN_DOWN it does not know is told by the check
        let mut odd = live(declared("hold-odd", &url, "WHEN_DOWN = \"pause\"\n"));
        let problems = odd.check_invokes(&prog);
        assert!(problems.iter().any(|p| p.contains("WHEN_DOWN")), "{problems:?}");
    }

    #[test]
    fn a_caught_failure_is_a_step_and_the_run_goes_on() {
        let prog = parse("try:\n    x = event.n.deep\ncatch e:\n    emit \"vx.r\", {}\nend\nemit \"vx.after\", {}\n").unwrap();
        let mut engine = Engine::live(std::env::temp_dir(), "default".into());
        engine.record_steps(true);
        run(&prog, &json!({"n": 5}), &mut engine).unwrap();
        assert_eq!(steps_of(&mut engine), ["1", "2 caught", "4>vx.r", "6>vx.after"]);
        // a catch that fails ends the run on its own statement
        let prog = parse("try:\n    x = event.n.deep\ncatch e:\n    y = e.error.x\nend\n").unwrap();
        engine.record_steps(true);
        failed(run(&prog, &json!({"n": 5}), &mut engine));
        assert_eq!(steps_of(&mut engine), ["1", "2 caught", "4!"]);
    }

    /// A memory that remembers what the test gives it.
    struct Remembered(std::sync::Mutex<Vec<String>>);

    impl Memory for Remembered {
        fn seen(&self, key: &str) -> Result<bool, String> {
            if key == "broken" {
                return Err("down".into());
            }
            Ok(self.0.lock().unwrap().iter().any(|k| k == key))
        }
    }

    #[test]
    fn seen_reads_the_memory_and_remember_leaves_its_marks_to_the_runtime() {
        let prog = parse(
            r#"KEEP = "7d"
if seen(event.key):
    emit "vx.dup", {key: event.key}
else:
    o = invoke mark_it(key: event.key)
    emit "vx.new", {key: event.key}
    remember event.key for KEEP
end
"#,
        )
        .unwrap();
        let root = std::env::temp_dir().join(format!("vejas-memory-{}", std::process::id()));
        std::fs::create_dir_all(root.join("services")).unwrap();
        std::fs::write(root.join("services").join("mark_it.vjs"), "remember f\"svc:{key}\" for \"1h\"\n").unwrap();
        let memory = Arc::new(Remembered(std::sync::Mutex::new(vec!["A".into()])));
        let mut engine = Engine::live(root.clone(), "default".into());
        engine.remember_in(Some(memory.clone()));
        let ctx = run(&prog, &json!({"key": "A"}), &mut engine).unwrap();
        assert_eq!(ctx.emits[0].0, "vx.dup");
        assert!(ctx.marks.is_empty());
        let ctx = run(&prog, &json!({"key": "B"}), &mut engine).unwrap();
        assert_eq!(ctx.emits[0].0, "vx.new");
        let hour = std::time::Duration::from_secs(3600);
        assert_eq!(ctx.marks, [("svc:B".to_string(), hour), ("B".to_string(), 168 * hour)]);
        // a key is any value but null: a number reads as its text
        assert_eq!(failed(run(&prog, &json!({}), &mut engine)), "seen() takes a key, and this one is null");
        assert_eq!(run(&prog, &json!({"key": 42}), &mut engine).unwrap().marks[1].0, "42");
        let e = failed(run(&parse("remember event.k for \"1d\"\n").unwrap(), &json!({}), &mut engine));
        assert_eq!(e, "remember takes a key, and this one is null");
        // a memory that does not answer fails the run, and a later delivery may read it
        assert_eq!(failed(run(&prog, &json!({"key": "broken"}), &mut engine)), "seen(): down");
        assert!(engine.take_failure().is_some_and(|f| f.retry && f.attempts == vejas_connector::MAX_DELIVERIES));
        // a run with no memory says so
        let e = failed(run(&prog, &json!({"key": "B"}), &mut Engine::live(root.clone(), "default".into())));
        assert!(e.contains("this run has no memory"), "{e}");
        // a run that is shown reads no state: every key is new to it
        let mut shown = Engine::shadow(root, "default".into());
        shown.remember_in(Some(memory));
        assert_eq!(run(&prog, &json!({"key": "A"}), &mut shown).unwrap().emits[0].0, "vx.new");
        // remember is a write for the write gate, seen a read
        assert!(parse("remember event.k for \"1d\"\n").unwrap().writes_bus());
        assert!(!parse("x = seen(event.k)\nrespond 200, {seen: x}\n").unwrap().writes_bus());
        assert!(parse("try:\n    x = 1\ncatch e:\n    remember event.k for \"1d\"\nend\n").unwrap().writes_bus());
    }

    #[test]
    fn what_the_runtime_did_not_give_waits_for_the_last_delivery() {
        // a secret store that does not answer may answer a later delivery
        let prog = parse(
            r#"try:
    k = secret("absent/key")
    emit "vx.ok", {}
catch e:
    emit "vx.r", {service: e.service, status: e.status, attempts: e.attempts}
end
"#,
        )
        .unwrap();
        let mut engine = Engine::with_secrets(std::env::temp_dir(), "default".into(), Arc::new(Unreachable));
        engine.delivered(Some(1));
        failed(run(&prog, &json!({}), &mut engine));
        let failure = engine.take_failure().expect("a failure a later delivery may change");
        assert!(failure.retry && failure.attempts == vejas_connector::MAX_DELIVERIES);
        assert!(!failure.delay.is_zero(), "a nak with no delay spends the deliveries at once");
        engine.delivered(Some(vejas_connector::MAX_DELIVERIES));
        let ctx = run(&prog, &json!({}), &mut engine).unwrap();
        assert_eq!(
            ctx.emits,
            [("vx.r".to_string(), json!({"service": null, "status": null, "attempts": vejas_connector::MAX_DELIVERIES}))]
        );
        // a store that answers there is no such secret: no delivery changes it
        let mut none = Engine::with_secrets(std::env::temp_dir(), "default".into(), Arc::new(MapStore(Default::default())));
        none.delivered(Some(1));
        assert_eq!(run(&prog, &json!({}), &mut none).unwrap().emits[0].0, "vx.r");
        // a declaration whose secret does not resolve names the service as
        // the flow does, as a call's failure does
        let (url, _) = system(|_| (200, "{}", 0));
        let call = parse(
            r#"try:
    o = invoke get_order(id: event.id)
catch e:
    emit "vx.r", {service: e.service, attempts: e.attempts}
end
"#,
        )
        .unwrap();
        let root = declared("unresolved", &url, "");
        // a secret the store says it does not have: no delivery changes it
        let mut bare = Engine::with_secrets(root.clone(), "default".into(), Arc::new(MapStore(Default::default())));
        bare.delivered(Some(1));
        let ctx = run(&call, &json!({"id": "x"}), &mut bare).unwrap();
        assert_eq!(ctx.emits[0].1, json!({"service": "get_order", "attempts": 1}));
        // a store that does not answer: the catch waits for the last delivery
        let mut down = Engine::with_secrets(root, "default".into(), Arc::new(Unreachable));
        down.delivered(Some(1));
        failed(run(&call, &json!({"id": "x"}), &mut down));
        let failure = down.take_failure().expect("a failure a later delivery may change");
        assert_eq!((failure.service.as_str(), failure.attempts), ("get_order", vejas_connector::MAX_DELIVERIES));
        assert!(!failure.delay.is_zero());
        down.delivered(Some(vejas_connector::MAX_DELIVERIES));
        let ctx = run(&call, &json!({"id": "x"}), &mut down).unwrap();
        assert_eq!(ctx.emits[0].1, json!({"service": "get_order", "attempts": vejas_connector::MAX_DELIVERIES}));
    }

    /// A secret store that does not answer.
    struct Unreachable;
    impl crate::secrets::SecretStore for Unreachable {
        fn kind(&self) -> &'static str {
            "unreachable"
        }
        fn get(&self, _reference: &str) -> Result<String, String> {
            Err("vault: connection refused".into())
        }
        fn read(&self, reference: &str) -> Result<String, crate::secrets::Missing> {
            self.get(reference).map_err(|error| crate::secrets::Missing { error, later: true })
        }
    }

    #[test]
    fn a_filter_whose_condition_fails_gives_the_flow_its_variables_back() {
        let p = one_emit(
            r#"id = event.id
try:
    open = event.lines[qty.n > 0]
catch e:
    emit "vx.rejected", {order: id}
end"#,
            json!({"id": "O-1", "lines": [{"id": "L-9", "qty": 3}]}),
        );
        assert_eq!(p["order"], "O-1");
    }

    #[test]
    fn what_a_try_sent_before_its_failure_is_no_step_that_sent() {
        let prog = parse(
            r#"try:
    emit "vx.inside", {}
    respond 201, {}
    x = event.n.deep
catch e:
    emit "vx.r", {}
end
"#,
        )
        .unwrap();
        let mut engine = Engine::live(std::env::temp_dir(), "default".into());
        engine.record_steps(true);
        run(&prog, &json!({"n": 5}), &mut engine).unwrap();
        assert_eq!(steps_of(&mut engine), ["1", "2", "3", "4 caught", "6>vx.r"]);
    }

    #[test]
    fn a_window_a_constant_holds_is_its_literal_whatever_the_event_says() {
        // the constant is assigned where it is written, after the remember:
        // its literal holds all the same, and an event's field of its name
        // does not
        let prog = parse("remember event.id for KEEP\nKEEP = \"1h\"\n").unwrap();
        let ctx = run(&prog, &json!({"id": "a", "KEEP": "3650d"}), &mut Engine::live(std::env::temp_dir(), "default".into())).unwrap();
        assert_eq!(ctx.marks, [("a".to_string(), std::time::Duration::from_secs(3600))]);
        // a constant the flow sets elsewhere too would not be the window
        for src in [
            "KEEP = \"1h\"\nif event.vip:\n    KEEP = \"30d\"\nend\nremember event.id for KEEP\n",
            "KEEP = \"1h\"\nfor KEEP in event.all:\n    x = 1\nend\nremember event.id for KEEP\n",
            "KEEP = \"1h\"\ntry:\n    x = 1\ncatch KEEP:\n    x = 2\nend\nremember event.id for KEEP\n",
        ] {
            let e = parse(src).err().unwrap_or_else(|| panic!("{src:?} parsed"));
            assert!(e.contains("sets KEEP elsewhere too"), "{e}");
        }
    }
}
