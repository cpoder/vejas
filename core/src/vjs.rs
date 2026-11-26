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
    let b = src.as_bytes();
    let mut out = Vec::new();
    // a byte-order mark, which some editors write, is not code
    let mut i = if b.starts_with(&[0xEF, 0xBB, 0xBF]) { 3 } else { 0 };
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
    /// (method, path) here, binds {path params} into the event, and returns the
    /// flow's `respond`. Several flows (one per verb) compose a REST resource.
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
            Stmt::Assign(_, _) | Stmt::InvokeMerge(_, _) => {}
            // nested control flow makes the arm advanced → RAW
            Stmt::If(..) | Stmt::For(..) => return None,
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
            Stmt::Emit(..) | Stmt::Respond(..) | Stmt::InvokeMerge(..) => {}
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
        Stmt::Emit(a, b) | Stmt::Respond(a, b) => reads_whole_event(a) || reads_whole_event(b),
        Stmt::InvokeMerge(_, args) => args.iter().any(|(_, v)| reads_whole_event(v)),
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
            Stmt::Emit(a, b) | Stmt::Respond(a, b) => {
                collect_reads(a, set, out);
                collect_reads(b, set, out);
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
            Stmt::Emit(a, b) | Stmt::Respond(a, b) => {
                collect_reads(a, own, out);
                collect_reads(b, own, out);
            }
            Stmt::InvokeMerge(_, args) => args.iter().for_each(|(_, v)| collect_reads(v, own, out)),
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
            Stmt::Assign(..) | Stmt::Respond(..) | Stmt::InvokeMerge(..) => {}
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
            Stmt::If(..) => steps.push(json!({"kind": "when", "line": line, "code": text})),
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
                            let mut sub = Parser { toks: &toks, i: 0, src: self.src, newlines: self.newlines };
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
    let mut p = Parser { toks: &toks, i: 0, src, newlines: &newlines };
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
}

/// A service as the engine runs it: its statements, and the version of the
/// source they came from, which the trace record joins the code with.
struct Service {
    stmts: Block,
    /// `package:service`, as its steps name it.
    name: Arc<str>,
    /// Its source, hashed into its version the first time a run that records
    /// steps invokes it: a run that records none never pays for the hash.
    src: Box<str>,
    version: std::sync::OnceLock<Arc<str>>,
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
        Self::with_secrets(project_root, caller_pkg, Arc::new(store))
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
        }
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
    /// The names an invoked service gives a value to: they land in the
    /// caller's pipeline (the flow page says where a value comes from).
    pub fn service_makes(&mut self, name: &str) -> Result<Vec<String>, String> {
        let (pkg, svc) = self.resolve(name)?;
        let service = self.service(&pkg, &svc)?;
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
        let service = Arc::new(Service {
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
}

pub fn run(prog: &Program, event: &Value, engine: &mut Engine) -> Result<Ctx, String> {
    let mut vars = Map::new();
    if let Value::Object(obj) = event {
        for (k, v) in obj {
            vars.insert(k.clone(), v.clone());
        }
    }
    vars.insert("event".into(), event.clone());
    let mut ctx = Ctx { vars, emits: Vec::new(), response: None };
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
    let service = match engine.service(&pkg, &svc) {
        Ok(s) => s,
        Err(e) => {
            engine.depth -= 1;
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
    let mut ctx = Ctx { vars: args.clone(), emits: Vec::new(), response: None };
    ctx.vars.insert("event".into(), Value::Object(args));
    let result = exec_block(&service.stmts, &mut ctx, engine);
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
    emits.append(&mut ctx.emits);
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
            if name == "secret" {
                // resolve a secret at run time; never a literal, never exposed.
                // fail closed: a missing secret aborts the run.
                let reference = vals
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or("secret() needs a string reference, e.g. secret(\"slack/webhook\")")?;
                Value::String(engine.secrets.get(reference)?)
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
    let (vstart, vend) = entry.value_span;
    let vend = vend.min(src.len());
    let val_text = src[vstart..vend].trim_end();
    let parsed: Value = serde_json::from_str(val_text)
        .map_err(|_| format!("{name} has no key {key:?} and is not an editable table"))?;
    let obj = parsed
        .as_object()
        .ok_or_else(|| format!("{name} has no key {key:?} and is not a table — cannot add a key"))?;
    let close = vstart
        + val_text
            .rfind('}')
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
    /// respond, `enter p:s` a service starting, `p:s:2` a line in it, `...n`
    /// the steps left out.
    fn steps_of(engine: &mut Engine) -> Vec<String> {
        let steps = engine.take_steps().expect("the engine records steps");
        let one = |st: &Step| {
            let at = st.service.as_deref().map(|s| format!("{s}:")).unwrap_or_default();
            match &st.what {
                What::Line(l) => format!("{at}{l}"),
                What::Emit(l, subject) => format!("{at}{l}>{subject}"),
                What::Respond(l, status) => format!("{at}{l}<{status}"),
                What::Enter(_) => format!("enter {}", st.service.as_deref().unwrap_or("?")),
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
}
