//! Vejas SAP connector — native Rust over the SAP NW RFC SDK (`libsapnwrfc`).
//!
//! ADR-0014: no JVM, no Python; our code is Rust, the vendor library is the
//! official SAP C SDK. ADR-0011: this runs as an **isolated exec process**, the
//! FFI/`dlopen` lives here, never in the Vejas runtime — a native crash takes
//! down only this connector, which the runtime supervises and restarts.
//!
//! We `dlopen` `libsapnwrfc.so` at runtime (found via `SAP_LIB` or the loader's
//! `LD_LIBRARY_PATH`), so there is **no build-time SAP dependency**: the binary
//! builds anywhere and runs on any host where the SDK is present (it ships with
//! the SAP kernel).
//!
//! Protocol (socle exec-rpc): one JSON request per line on stdin, one JSON reply
//! per line on stdout. Requests:
//!   {"op":"ping"}
//!   {"op":"describe","func":"BAPI_USER_GETLIST"}
//!   {"op":"list","pattern":"STFC*"}
//!   {"op":"call","func":"RFC_READ_TABLE","import":{"QUERY_TABLE":"T000"},"max_rows":50}
//! `call` auto-marshals every EXPORT/CHANGING scalar/structure and every TABLES
//! parameter from the function's own metadata — the caller need not know types.
//! Replies: {"ok":true, ...} | {"ok":false,"stage":"...","code":N,"key":"...","message":"..."}
//!
//! Credentials come from the environment (the runtime injects `secret()` values,
//! never literals — ADR-0008): SAP_ASHOST, SAP_SYSNR, SAP_CLIENT, SAP_USER,
//! SAP_PASSWD, SAP_LANG.

use serde_json::{json, Value};
use std::ffi::{c_void, CString};
use std::io::{self, BufRead, Write};
use std::os::raw::{c_char, c_int, c_uint};

// ─────────────────────────── FFI: types ───────────────────────────
// SAP_UC on Linux is UTF-16LE (2 bytes). Proven against a live NPL system.
type SapUc = u16;
type RfcRc = c_int; // enum; 0 = RFC_OK
type RfcHandle = *mut c_void;

const RFC_BUFFER_TOO_SMALL: RfcRc = 23;

// RFC_DIRECTION
const RFC_IMPORT: c_uint = 1;
const RFC_EXPORT: c_uint = 2;
const RFC_CHANGING: c_uint = 3;
const RFC_TABLES: c_uint = 7;

// RFCTYPE (subset we name; others fall through to "TYPE<n>")
const RFCTYPE_STRUCTURE: c_uint = 17;

#[repr(C)]
struct RfcConnParam {
    name: *const SapUc,
    value: *const SapUc,
}

#[repr(C)]
struct RfcErrorInfo {
    code: RfcRc,
    group: c_int,
    key: [SapUc; 128],
    message: [SapUc; 512],
    abap_msg_class: [SapUc; 21],
    abap_msg_type: [SapUc; 2],
    abap_msg_number: [SapUc; 4],
    abap_msg_v1: [SapUc; 51],
    abap_msg_v2: [SapUc; 51],
    abap_msg_v3: [SapUc; 51],
    abap_msg_v4: [SapUc; 51],
}
impl RfcErrorInfo {
    fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
    fn to_json(&self, stage: &str) -> Value {
        json!({
            "ok": false,
            "stage": stage,
            "code": self.code,
            "group": self.group,
            "key": from_uc(&self.key),
            "message": from_uc(&self.message),
        })
    }
}

// RFC_PARAMETER_DESC — layout from sapnwrfc.h; #[repr(C)] matches the C ABI.
#[repr(C)]
struct RfcParameterDesc {
    name: [SapUc; 31],
    rtype: c_uint,     // RFCTYPE
    direction: c_uint, // RFC_DIRECTION
    nuc_length: c_uint,
    uc_length: c_uint,
    decimals: c_uint,
    type_desc_handle: RfcHandle,
    default_value: [SapUc; 31],
    parameter_text: [SapUc; 80],
    optional: u8,
    extended_description: *mut c_void,
}
impl RfcParameterDesc {
    fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

// RFC_FIELD_DESC — layout from sapnwrfc.h.
#[repr(C)]
struct RfcFieldDesc {
    name: [SapUc; 31],
    rtype: c_uint,
    nuc_length: c_uint,
    nuc_offset: c_uint,
    uc_length: c_uint,
    uc_offset: c_uint,
    decimals: c_uint,
    type_desc_handle: RfcHandle,
    extended_description: *mut c_void,
}
impl RfcFieldDesc {
    fn zeroed() -> Self {
        unsafe { std::mem::zeroed() }
    }
}

// ─────────────────────────── FFI: dlopen ───────────────────────────
extern "C" {
    fn dlopen(filename: *const c_char, flag: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlerror() -> *mut c_char;
}
const RTLD_NOW: c_int = 2;

type FnOpen = unsafe extern "C" fn(*const RfcConnParam, c_uint, *mut RfcErrorInfo) -> RfcHandle;
type FnPing = unsafe extern "C" fn(RfcHandle, *mut RfcErrorInfo) -> RfcRc;
type FnClose = unsafe extern "C" fn(RfcHandle, *mut RfcErrorInfo) -> RfcRc;
type FnGetFuncDesc = unsafe extern "C" fn(RfcHandle, *const SapUc, *mut RfcErrorInfo) -> RfcHandle;
type FnCreateFunc = unsafe extern "C" fn(RfcHandle, *mut RfcErrorInfo) -> RfcHandle;
type FnDestroyFunc = unsafe extern "C" fn(RfcHandle, *mut RfcErrorInfo) -> RfcRc;
type FnInvoke = unsafe extern "C" fn(RfcHandle, RfcHandle, *mut RfcErrorInfo) -> RfcRc;
type FnSetString =
    unsafe extern "C" fn(RfcHandle, *const SapUc, *const SapUc, c_uint, *mut RfcErrorInfo) -> RfcRc;
type FnGetString = unsafe extern "C" fn(
    RfcHandle,
    *const SapUc,
    *mut SapUc,
    c_uint,
    *mut c_uint,
    *mut RfcErrorInfo,
) -> RfcRc;
type FnGetParamCount = unsafe extern "C" fn(RfcHandle, *mut c_uint, *mut RfcErrorInfo) -> RfcRc;
type FnGetParamDescByIndex =
    unsafe extern "C" fn(RfcHandle, c_uint, *mut RfcParameterDesc, *mut RfcErrorInfo) -> RfcRc;
type FnGetFieldCount = unsafe extern "C" fn(RfcHandle, *mut c_uint, *mut RfcErrorInfo) -> RfcRc;
type FnGetFieldDescByIndex =
    unsafe extern "C" fn(RfcHandle, c_uint, *mut RfcFieldDesc, *mut RfcErrorInfo) -> RfcRc;
type FnDescribeType = unsafe extern "C" fn(RfcHandle, *mut RfcErrorInfo) -> RfcHandle;
type FnGetStructure =
    unsafe extern "C" fn(RfcHandle, *const SapUc, *mut RfcHandle, *mut RfcErrorInfo) -> RfcRc;
type FnGetTable =
    unsafe extern "C" fn(RfcHandle, *const SapUc, *mut RfcHandle, *mut RfcErrorInfo) -> RfcRc;
type FnGetRowCount = unsafe extern "C" fn(RfcHandle, *mut c_uint, *mut RfcErrorInfo) -> RfcRc;
type FnMoveTo = unsafe extern "C" fn(RfcHandle, c_uint, *mut RfcErrorInfo) -> RfcRc;
type FnGetCurrentRow = unsafe extern "C" fn(RfcHandle, *mut RfcErrorInfo) -> RfcHandle;

/// Function pointers resolved from `libsapnwrfc` at startup.
struct Sdk {
    open: FnOpen,
    ping: FnPing,
    close: FnClose,
    get_func_desc: FnGetFuncDesc,
    create_func: FnCreateFunc,
    destroy_func: FnDestroyFunc,
    invoke: FnInvoke,
    set_string: FnSetString,
    get_string: FnGetString,
    get_param_count: FnGetParamCount,
    get_param_desc: FnGetParamDescByIndex,
    get_field_count: FnGetFieldCount,
    get_field_desc: FnGetFieldDescByIndex,
    describe_type: FnDescribeType,
    get_structure: FnGetStructure,
    get_table: FnGetTable,
    get_row_count: FnGetRowCount,
    move_to: FnMoveTo,
    get_current_row: FnGetCurrentRow,
}

impl Sdk {
    fn load() -> Result<Sdk, String> {
        let lib = std::env::var("SAP_LIB").unwrap_or_else(|_| "libsapnwrfc.so".into());
        let cpath = CString::new(lib.clone()).unwrap();
        let handle = unsafe { dlopen(cpath.as_ptr(), RTLD_NOW) };
        if handle.is_null() {
            return Err(format!("dlopen({lib}) failed: {}", dl_err()));
        }
        macro_rules! sym {
            ($name:literal, $ty:ty) => {{
                let s = CString::new($name).unwrap();
                let p = unsafe { dlsym(handle, s.as_ptr()) };
                if p.is_null() {
                    return Err(format!("dlsym({}) failed: {}", $name, dl_err()));
                }
                unsafe { std::mem::transmute::<*mut c_void, $ty>(p) }
            }};
        }
        Ok(Sdk {
            open: sym!("RfcOpenConnection", FnOpen),
            ping: sym!("RfcPing", FnPing),
            close: sym!("RfcCloseConnection", FnClose),
            get_func_desc: sym!("RfcGetFunctionDesc", FnGetFuncDesc),
            create_func: sym!("RfcCreateFunction", FnCreateFunc),
            destroy_func: sym!("RfcDestroyFunction", FnDestroyFunc),
            invoke: sym!("RfcInvoke", FnInvoke),
            set_string: sym!("RfcSetString", FnSetString),
            get_string: sym!("RfcGetString", FnGetString),
            get_param_count: sym!("RfcGetParameterCount", FnGetParamCount),
            get_param_desc: sym!("RfcGetParameterDescByIndex", FnGetParamDescByIndex),
            get_field_count: sym!("RfcGetFieldCount", FnGetFieldCount),
            get_field_desc: sym!("RfcGetFieldDescByIndex", FnGetFieldDescByIndex),
            describe_type: sym!("RfcDescribeType", FnDescribeType),
            get_structure: sym!("RfcGetStructure", FnGetStructure),
            get_table: sym!("RfcGetTable", FnGetTable),
            get_row_count: sym!("RfcGetRowCount", FnGetRowCount),
            move_to: sym!("RfcMoveTo", FnMoveTo),
            get_current_row: sym!("RfcGetCurrentRow", FnGetCurrentRow),
        })
    }
}

fn dl_err() -> String {
    let p = unsafe { dlerror() };
    if p.is_null() {
        return "unknown".into();
    }
    unsafe { std::ffi::CStr::from_ptr(p) }
        .to_string_lossy()
        .into_owned()
}

// ─────────────────────────── UC helpers ───────────────────────────
/// UTF-8 str → NUL-terminated UTF-16 vector (kept alive by the caller).
fn to_uc(s: &str) -> Vec<SapUc> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}
/// UTF-16 buffer (NUL- or slice-terminated) → String, trailing blanks trimmed.
fn from_uc(buf: &[SapUc]) -> String {
    let end = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
    String::from_utf16_lossy(&buf[..end]).trim_end().to_string()
}

fn dir_name(d: c_uint) -> &'static str {
    match d {
        RFC_IMPORT => "import",
        RFC_EXPORT => "export",
        RFC_CHANGING => "changing",
        RFC_TABLES => "tables",
        _ => "?",
    }
}
fn type_name(t: c_uint) -> String {
    let s = match t {
        0 => "CHAR",
        1 => "DATE",
        2 => "BCD",
        3 => "TIME",
        4 => "BYTE",
        5 => "TABLE",
        6 => "NUM",
        7 => "FLOAT",
        8 => "INT",
        9 => "INT2",
        10 => "INT1",
        17 => "STRUCTURE",
        23 => "DECF16",
        24 => "DECF34",
        29 => "STRING",
        30 => "XSTRING",
        31 => "INT8",
        32 => "UTCLONG",
        _ => return format!("TYPE{t}"),
    };
    s.to_string()
}

// ─────────────────────────── connection ───────────────────────────
fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Open one RFC connection from the environment. Params proven against NPL.
fn open_connection(sdk: &Sdk) -> Result<RfcHandle, Value> {
    let pairs = [
        ("ashost", env_or("SAP_ASHOST", "localhost")),
        ("sysnr", env_or("SAP_SYSNR", "00")),
        ("client", env_or("SAP_CLIENT", "001")),
        ("user", env_or("SAP_USER", "")),
        ("passwd", env_or("SAP_PASSWD", "")),
        ("lang", env_or("SAP_LANG", "EN")),
    ];
    let bufs: Vec<(Vec<SapUc>, Vec<SapUc>)> =
        pairs.iter().map(|(k, v)| (to_uc(k), to_uc(v))).collect();
    let params: Vec<RfcConnParam> = bufs
        .iter()
        .map(|(k, v)| RfcConnParam {
            name: k.as_ptr(),
            value: v.as_ptr(),
        })
        .collect();

    let mut err = RfcErrorInfo::zeroed();
    let h = unsafe { (sdk.open)(params.as_ptr(), params.len() as c_uint, &mut err) };
    if h.is_null() {
        return Err(err.to_json("open"));
    }
    Ok(h)
}

// ─────────────────────── field-level reads ───────────────────────
/// Read any elementary field as a string (the SDK converts CHAR/NUM/DATE/INT/…).
fn get_string(sdk: &Sdk, container: RfcHandle, field: &str) -> Result<String, Value> {
    let name = to_uc(field);
    // Probe the length (bufferLength 0 → SDK reports needed stringLength).
    let mut needed: c_uint = 0;
    let mut e = RfcErrorInfo::zeroed();
    let mut tmp: [SapUc; 1] = [0];
    let rc = unsafe {
        (sdk.get_string)(container, name.as_ptr(), tmp.as_mut_ptr(), 0, &mut needed, &mut e)
    };
    if rc != 0 && rc != RFC_BUFFER_TOO_SMALL {
        return Err(e.to_json(&format!("get_string:{field}")));
    }
    let mut buf = vec![0 as SapUc; (needed as usize) + 2];
    let mut got: c_uint = 0;
    let mut e2 = RfcErrorInfo::zeroed();
    let rc2 = unsafe {
        (sdk.get_string)(
            container,
            name.as_ptr(),
            buf.as_mut_ptr(),
            buf.len() as c_uint,
            &mut got,
            &mut e2,
        )
    };
    if rc2 != 0 {
        return Err(e2.to_json(&format!("get_string:{field}")));
    }
    let n = (got as usize).min(buf.len());
    Ok(from_uc(&buf[..n]))
}

/// Column names of a structure/row handle, from its type description.
fn field_names(sdk: &Sdk, container: RfcHandle) -> Result<Vec<String>, Value> {
    let mut e = RfcErrorInfo::zeroed();
    let td = unsafe { (sdk.describe_type)(container, &mut e) };
    if td.is_null() {
        return Err(e.to_json("describe_type"));
    }
    let mut count: c_uint = 0;
    let mut e2 = RfcErrorInfo::zeroed();
    if unsafe { (sdk.get_field_count)(td, &mut count, &mut e2) } != 0 {
        return Err(e2.to_json("get_field_count"));
    }
    let mut names = Vec::with_capacity(count as usize);
    for i in 0..count {
        let mut fd = RfcFieldDesc::zeroed();
        let mut e3 = RfcErrorInfo::zeroed();
        if unsafe { (sdk.get_field_desc)(td, i, &mut fd, &mut e3) } != 0 {
            return Err(e3.to_json("get_field_desc"));
        }
        names.push(from_uc(&fd.name));
    }
    Ok(names)
}

/// Read a structure container into a JSON object (elementary fields as strings;
/// nested struct/table fields that don't read as string become null).
fn read_structure(sdk: &Sdk, container: RfcHandle) -> Result<Value, Value> {
    let cols = field_names(sdk, container)?;
    let mut obj = serde_json::Map::new();
    for c in &cols {
        let v = match get_string(sdk, container, c) {
            Ok(s) => Value::String(s),
            Err(_) => Value::Null,
        };
        obj.insert(c.clone(), v);
    }
    Ok(Value::Object(obj))
}

/// Read an export/changing STRUCTURE parameter by name.
fn marshal_structure(sdk: &Sdk, container: RfcHandle, name: &str) -> Result<Value, Value> {
    let n = to_uc(name);
    let mut h: RfcHandle = std::ptr::null_mut();
    let mut e = RfcErrorInfo::zeroed();
    if unsafe { (sdk.get_structure)(container, n.as_ptr(), &mut h, &mut e) } != 0 {
        return Err(e.to_json(&format!("get_structure:{name}")));
    }
    read_structure(sdk, h)
}

/// Read a TABLES parameter into a JSON array of row objects (capped at max_rows).
fn marshal_table(sdk: &Sdk, container: RfcHandle, name: &str, max_rows: usize) -> Result<Value, Value> {
    let n = to_uc(name);
    let mut table: RfcHandle = std::ptr::null_mut();
    let mut e = RfcErrorInfo::zeroed();
    if unsafe { (sdk.get_table)(container, n.as_ptr(), &mut table, &mut e) } != 0 {
        return Err(e.to_json(&format!("get_table:{name}")));
    }
    let mut rows: c_uint = 0;
    let mut e2 = RfcErrorInfo::zeroed();
    if unsafe { (sdk.get_row_count)(table, &mut rows, &mut e2) } != 0 {
        return Err(e2.to_json(&format!("get_row_count:{name}")));
    }
    let total = rows as usize;
    if total == 0 {
        return Ok(json!([]));
    }
    // Columns from the first row.
    let mut e3 = RfcErrorInfo::zeroed();
    if unsafe { (sdk.move_to)(table, 0, &mut e3) } != 0 {
        return Err(e3.to_json(&format!("move_to:{name}")));
    }
    let first = unsafe { (sdk.get_current_row)(table, &mut RfcErrorInfo::zeroed()) };
    if first.is_null() {
        return Err(json!({"ok": false, "stage": format!("get_current_row:{name}")}));
    }
    let cols = field_names(sdk, first)?;

    let limit = total.min(max_rows);
    let mut arr = Vec::with_capacity(limit);
    for r in 0..limit {
        let mut er = RfcErrorInfo::zeroed();
        if unsafe { (sdk.move_to)(table, r as c_uint, &mut er) } != 0 {
            return Err(er.to_json(&format!("move_to:{name}")));
        }
        let row = unsafe { (sdk.get_current_row)(table, &mut RfcErrorInfo::zeroed()) };
        if row.is_null() {
            break;
        }
        let mut obj = serde_json::Map::new();
        for c in &cols {
            let v = match get_string(sdk, row, c) {
                Ok(s) => Value::String(s),
                Err(_) => Value::Null,
            };
            obj.insert(c.clone(), v);
        }
        arr.push(Value::Object(obj));
    }
    Ok(json!({"rows": Value::Array(arr), "total": total, "returned": limit}))
}

// ─────────────────────────── ops ───────────────────────────
fn op_ping(sdk: &Sdk, conn: RfcHandle) -> Value {
    let mut err = RfcErrorInfo::zeroed();
    if unsafe { (sdk.ping)(conn, &mut err) } != 0 {
        return err.to_json("ping");
    }
    json!({"ok": true})
}

/// Enumerate a function's parameters via metadata (no invoke). Powers `describe`
/// and the auto-marshalling in `call`.
fn parameters(sdk: &Sdk, desc: RfcHandle) -> Result<Vec<RfcParameterDesc>, Value> {
    let mut count: c_uint = 0;
    let mut e = RfcErrorInfo::zeroed();
    if unsafe { (sdk.get_param_count)(desc, &mut count, &mut e) } != 0 {
        return Err(e.to_json("get_param_count"));
    }
    let mut out = Vec::with_capacity(count as usize);
    for i in 0..count {
        let mut pd = RfcParameterDesc::zeroed();
        let mut e2 = RfcErrorInfo::zeroed();
        if unsafe { (sdk.get_param_desc)(desc, i, &mut pd, &mut e2) } != 0 {
            return Err(e2.to_json("get_param_desc"));
        }
        out.push(pd);
    }
    Ok(out)
}

fn func_desc(sdk: &Sdk, conn: RfcHandle, func: &str) -> Result<RfcHandle, Value> {
    let name = to_uc(func);
    let mut e = RfcErrorInfo::zeroed();
    let d = unsafe { (sdk.get_func_desc)(conn, name.as_ptr(), &mut e) };
    if d.is_null() {
        return Err(e.to_json("get_func_desc"));
    }
    Ok(d)
}

fn op_describe(sdk: &Sdk, conn: RfcHandle, req: &Value) -> Value {
    let Some(func) = req.get("func").and_then(|v| v.as_str()) else {
        return json!({"ok": false, "error": "missing 'func'"});
    };
    let desc = match func_desc(sdk, conn, func) {
        Ok(d) => d,
        Err(e) => return e,
    };
    let params = match parameters(sdk, desc) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let list: Vec<Value> = params
        .iter()
        .map(|p| {
            json!({
                "name": from_uc(&p.name),
                "direction": dir_name(p.direction),
                "type": type_name(p.rtype),
                "length": p.uc_length,
                "decimals": p.decimals,
                "optional": p.optional != 0,
                "text": from_uc(&p.parameter_text),
            })
        })
        .collect();
    json!({"ok": true, "func": func, "parameters": list})
}

/// Invoke a function module and auto-marshal every output parameter.
fn op_call(sdk: &Sdk, conn: RfcHandle, req: &Value) -> Value {
    let Some(func) = req.get("func").and_then(|v| v.as_str()) else {
        return json!({"ok": false, "error": "missing 'func'"});
    };
    let max_rows = req
        .get("max_rows")
        .and_then(|v| v.as_u64())
        .unwrap_or(1000) as usize;

    let desc = match func_desc(sdk, conn, func) {
        Ok(d) => d,
        Err(e) => return e,
    };
    let mut e = RfcErrorInfo::zeroed();
    let f = unsafe { (sdk.create_func)(desc, &mut e) };
    if f.is_null() {
        return e.to_json("create_func");
    }

    // Set scalar imports. (Import tables/structures: a later increment.)
    if let Some(imports) = req.get("import").and_then(|v| v.as_object()) {
        for (k, v) in imports {
            if v.is_array() || v.is_object() {
                continue; // non-scalar import not yet supported
            }
            let val = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            let ku = to_uc(k);
            let vu = to_uc(&val);
            let len = vu.len().saturating_sub(1) as c_uint; // exclude NUL
            let mut ei = RfcErrorInfo::zeroed();
            if unsafe { (sdk.set_string)(f, ku.as_ptr(), vu.as_ptr(), len, &mut ei) } != 0 {
                unsafe { (sdk.destroy_func)(f, &mut RfcErrorInfo::zeroed()) };
                return ei.to_json(&format!("set_string:{k}"));
            }
        }
    }

    // Invoke.
    let mut ei = RfcErrorInfo::zeroed();
    if unsafe { (sdk.invoke)(conn, f, &mut ei) } != 0 {
        unsafe { (sdk.destroy_func)(f, &mut RfcErrorInfo::zeroed()) };
        return ei.to_json("invoke");
    }

    // Auto-marshal outputs from metadata.
    let params = match parameters(sdk, desc) {
        Ok(p) => p,
        Err(je) => {
            unsafe { (sdk.destroy_func)(f, &mut RfcErrorInfo::zeroed()) };
            return je;
        }
    };
    let mut export = serde_json::Map::new();
    let mut tables = serde_json::Map::new();
    for p in &params {
        let name = from_uc(&p.name);
        match p.direction {
            RFC_TABLES => match marshal_table(sdk, f, &name, max_rows) {
                Ok(v) => {
                    tables.insert(name, v);
                }
                Err(je) => {
                    unsafe { (sdk.destroy_func)(f, &mut RfcErrorInfo::zeroed()) };
                    return je;
                }
            },
            RFC_EXPORT | RFC_CHANGING => {
                let v = if p.rtype == RFCTYPE_STRUCTURE {
                    marshal_structure(sdk, f, &name).unwrap_or(Value::Null)
                } else {
                    match get_string(sdk, f, &name) {
                        Ok(s) => Value::String(s),
                        Err(_) => Value::Null,
                    }
                };
                export.insert(name, v);
            }
            _ => {}
        }
    }

    unsafe { (sdk.destroy_func)(f, &mut RfcErrorInfo::zeroed()) };
    json!({"ok": true, "func": func, "export": Value::Object(export), "tables": Value::Object(tables)})
}

/// List function modules matching a pattern, via the ABAP `RFC_FUNCTION_SEARCH`.
fn op_list(sdk: &Sdk, conn: RfcHandle, req: &Value) -> Value {
    let pattern = req
        .get("pattern")
        .and_then(|v| v.as_str())
        .unwrap_or("*")
        .to_string();
    let call = json!({
        "func": "RFC_FUNCTION_SEARCH",
        "import": {"FUNCNAME": pattern},
        "max_rows": req.get("max_rows").cloned().unwrap_or(json!(200)),
    });
    let mut res = op_call(sdk, conn, &call);
    // Surface the FUNCTIONS table at top level for convenience.
    if let Some(obj) = res.as_object_mut() {
        if let Some(tables) = obj.get("tables").and_then(|t| t.as_object()) {
            if let Some(funcs) = tables.get("FUNCTIONS").cloned() {
                obj.insert("functions".into(), funcs);
            }
        }
    }
    res
}

// ─────────────────────────── main loop ───────────────────────────
fn reply(v: &Value) {
    let mut out = io::stdout().lock();
    let _ = writeln!(out, "{v}");
    let _ = out.flush();
}

fn main() {
    let sdk = match Sdk::load() {
        Ok(s) => s,
        Err(e) => {
            reply(&json!({"ok": false, "fatal": true, "error": e}));
            std::process::exit(1);
        }
    };
    let conn = match open_connection(&sdk) {
        Ok(c) => c,
        Err(je) => {
            reply(&je);
            std::process::exit(1);
        }
    };
    reply(&json!({"ok": true, "ready": true, "connector": "sap-rfc"}));

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let req: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(err) => {
                reply(&json!({"ok": false, "error": format!("bad json: {err}")}));
                continue;
            }
        };
        let op = req.get("op").and_then(|v| v.as_str()).unwrap_or("");
        let out = match op {
            "ping" => op_ping(&sdk, conn),
            "describe" => op_describe(&sdk, conn, &req),
            "list" => op_list(&sdk, conn, &req),
            "call" => op_call(&sdk, conn, &req),
            other => json!({"ok": false, "error": format!("unknown op: {other}")}),
        };
        let out = match (req.get("id"), out) {
            (Some(id), Value::Object(mut m)) => {
                m.insert("id".into(), id.clone());
                Value::Object(m)
            }
            (_, o) => o,
        };
        reply(&out);
    }

    unsafe { (sdk.close)(conn, &mut RfcErrorInfo::zeroed()) };
}
