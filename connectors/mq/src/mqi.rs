//! The MQI FFI — hand-declared, `dlopen`'d at runtime, never in the Vejas core.
//!
//! IBM ships no pure-Rust MQ client; the supported path is the C MQI. Rather than
//! a build-time `-sys` crate (ADR-0023 open question 1), we take the move the SAP
//! connector already made (ADR-0014): declare the handful of MQI calls and their
//! structures as `#[repr(C)]` and resolve the symbols from `libmqic_r.so` at
//! startup. The crate then builds with NO MQ headers or libraries present; only
//! *running* needs the client, installed where the recipe runs.
//!
//! The struct layouts are the version-1 shapes from `cmqc.h` — stable since MQ v5.
//! We set each descriptor's `Version` field to 1 so the queue manager reads only
//! the fields declared here; later-version tails are deliberately absent. These
//! layouts are verified by published spec, not against a live queue manager in CI
//! (ADR-0023: real-QM certification is the declared exception, like SAP).

#![allow(non_snake_case, non_upper_case_globals, dead_code)]

use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_void};
use std::time::Duration;

// MQI scalar types.
type MqLong = i32; // MQLONG — 32-bit signed
type MqHconn = i32; // MQHCONN — connection handle
type MqHobj = i32; // MQHOBJ — object handle

// ───────────────────────── constants (cmqc.h) ─────────────────────────
// Completion codes / reasons.
pub const MQCC_OK: MqLong = 0;
pub const MQCC_WARNING: MqLong = 1;
pub const MQCC_FAILED: MqLong = 2;
pub const MQRC_NONE: MqLong = 0;
pub const MQRC_NO_MSG_AVAILABLE: MqLong = 2033;
pub const MQRC_TRUNCATED_MSG_FAILED: MqLong = 2080;

// Struct versions.
const MQCNO_VERSION_1: MqLong = 1;
const MQCNO_VERSION_5: MqLong = 5;
const MQCSP_VERSION_1: MqLong = 1;
const MQCSP_AUTH_NONE: MqLong = 0;
const MQCSP_AUTH_USER_ID_AND_PWD: MqLong = 1;
const MQOD_VERSION_1: MqLong = 1;
const MQMD_VERSION_1: MqLong = 1;
const MQGMO_VERSION_1: MqLong = 1;
const MQPMO_VERSION_1: MqLong = 1;

// Object type / open options.
const MQOT_Q: MqLong = 1;
const MQOO_INPUT_AS_Q_DEF: MqLong = 0x0000_0001;
const MQOO_OUTPUT: MqLong = 0x0000_0010;
const MQOO_FAIL_IF_QUIESCING: MqLong = 0x0000_2000;
const MQCO_NONE: MqLong = 0;

// Connect options.
const MQCNO_NONE: MqLong = 0;

// Get-message options.
const MQGMO_WAIT: MqLong = 0x0000_0001;
const MQGMO_SYNCPOINT: MqLong = 0x0000_0002;
const MQGMO_FAIL_IF_QUIESCING: MqLong = 0x0000_2000;
const MQWI_UNLIMITED: MqLong = -1;

// Put-message options.
const MQPMO_SYNCPOINT: MqLong = 0x0000_0002;
const MQPMO_FAIL_IF_QUIESCING: MqLong = 0x0000_2000;
const MQPMO_NEW_MSG_ID: MqLong = 0x0000_0040;

// Message type / format.
const MQMT_DATAGRAM: MqLong = 8;
const MQFMT_STRING: &[u8; 8] = b"MQSTR   ";
const MQFMT_NONE: &[u8; 8] = b"        ";

// ───────────────────────── descriptor structs ─────────────────────────
// Every char array is fixed-size and space-padded per MQI convention. Layouts are
// the v1 shapes; field order and widths are the ABI contract — do not reorder.

// The v5 layout (fields through SecurityParmsPtr) so we can point at an MQCSP for
// user/password auth. The struct is physically v5-sized, but `Version` governs what
// the queue manager reads: it stays 1 (v1 behaviour) unless credentials are set, in
// which case it becomes 5 and SecurityParmsPtr is honoured. Field order/width is the
// cmqc.h ABI — do not reorder (verified by the size assertion below).
#[repr(C)]
struct Mqcno {
    StrucId: [c_char; 4],          // "CNO "
    Version: MqLong,               // 1, or 5 when SecurityParmsPtr is set
    Options: MqLong,               // MQCNO_NONE
    ClientConnOffset: MqLong,      // v2
    ClientConnPtr: *mut c_void,    // v2 (MQPTR)
    ConnTag: [u8; 128],            // v3 (MQBYTE128)
    SSLConfigPtr: *mut c_void,     // v4 (MQPTR)
    SSLConfigOffset: MqLong,       // v4
    SecurityParmsOffset: MqLong,   // v5
    SecurityParmsPtr: *mut c_void, // v5 (MQPTR → MQCSP)
}
impl Default for Mqcno {
    fn default() -> Self {
        Mqcno {
            StrucId: cc(b"CNO "),
            Version: MQCNO_VERSION_1,
            Options: MQCNO_NONE,
            ClientConnOffset: 0,
            ClientConnPtr: std::ptr::null_mut(),
            ConnTag: [0; 128],
            SSLConfigPtr: std::ptr::null_mut(),
            SSLConfigOffset: 0,
            SecurityParmsOffset: 0,
            SecurityParmsPtr: std::ptr::null_mut(),
        }
    }
}

/// Connection Security Parameters — carries the user id and password for MQCONNX
/// authentication (most enterprise queue managers set CHCKCLNT(REQUIRED)). The
/// strings are passed by pointer+length (not null-terminated); the buffers must
/// outlive the MQCONNX call.
#[repr(C)]
struct Mqcsp {
    StrucId: [c_char; 4],           // "CSP "
    Version: MqLong,                // 1
    AuthenticationType: MqLong,     // MQCSP_AUTH_USER_ID_AND_PWD
    Reserved1: [u8; 4],
    CSPUserIdPtr: *mut c_void,      // MQPTR
    CSPUserIdOffset: MqLong,
    CSPUserIdLength: MqLong,
    Reserved2: [u8; 8],
    CSPPasswordPtr: *mut c_void,    // MQPTR
    CSPPasswordOffset: MqLong,
    CSPPasswordLength: MqLong,
}
impl Default for Mqcsp {
    fn default() -> Self {
        Mqcsp {
            StrucId: cc(b"CSP "),
            Version: MQCSP_VERSION_1,
            AuthenticationType: MQCSP_AUTH_NONE,
            Reserved1: [0; 4],
            CSPUserIdPtr: std::ptr::null_mut(),
            CSPUserIdOffset: 0,
            CSPUserIdLength: 0,
            Reserved2: [0; 8],
            CSPPasswordPtr: std::ptr::null_mut(),
            CSPPasswordOffset: 0,
            CSPPasswordLength: 0,
        }
    }
}

#[repr(C)]
struct Mqod {
    StrucId: [c_char; 4], // "OD  "
    Version: MqLong,      // 1
    ObjectType: MqLong,   // MQOT_Q
    ObjectName: [c_char; 48],
    ObjectQMgrName: [c_char; 48],
    DynamicQName: [c_char; 48],
    AlternateUserId: [c_char; 12],
}
impl Default for Mqod {
    fn default() -> Self {
        Mqod {
            StrucId: cc(b"OD  "),
            Version: MQOD_VERSION_1,
            ObjectType: MQOT_Q,
            ObjectName: [b' ' as c_char; 48],
            ObjectQMgrName: [b' ' as c_char; 48],
            DynamicQName: cc48(b"AMQ.*"),
            AlternateUserId: [b' ' as c_char; 12],
        }
    }
}

#[repr(C)]
struct Mqmd {
    StrucId: [c_char; 4], // "MD  "
    Version: MqLong,      // 1
    Report: MqLong,
    MsgType: MqLong,
    Expiry: MqLong,
    Feedback: MqLong,
    Encoding: MqLong,
    CodedCharSetId: MqLong,
    Format: [c_char; 8],
    Priority: MqLong,
    Persistence: MqLong,
    MsgId: [u8; 24],
    CorrelId: [u8; 24],
    BackoutCount: MqLong,
    ReplyToQ: [c_char; 48],
    ReplyToQMgr: [c_char; 48],
    UserIdentifier: [c_char; 12],
    AccountingToken: [u8; 32],
    ApplIdentityData: [c_char; 32],
    PutApplType: MqLong,
    PutApplName: [c_char; 28],
    PutDate: [c_char; 8],
    PutTime: [c_char; 8],
    ApplOriginData: [c_char; 4],
}
impl Default for Mqmd {
    fn default() -> Self {
        // MQCCSI_Q_MGR = 0 lets the queue manager set the CCSID; MQENC_NATIVE via 0
        // is not correct in general, but for a byte-transparent JSON body we send
        // MQFMT_STRING and let the QM's default encoding apply.
        Mqmd {
            StrucId: cc(b"MD  "),
            Version: MQMD_VERSION_1,
            Report: 0,
            MsgType: MQMT_DATAGRAM,
            Expiry: -1, // MQEI_UNLIMITED
            Feedback: 0,
            Encoding: 273, // MQENC_NATIVE on little-endian (0x111)
            CodedCharSetId: 0, // MQCCSI_Q_MGR
            Format: cc8(MQFMT_STRING),
            Priority: -1, // MQPRI_PRIORITY_AS_Q_DEF
            Persistence: 2, // MQPER_PERSISTENCE_AS_Q_DEF
            MsgId: [0; 24],
            CorrelId: [0; 24],
            BackoutCount: 0,
            ReplyToQ: [b' ' as c_char; 48],
            ReplyToQMgr: [b' ' as c_char; 48],
            UserIdentifier: [b' ' as c_char; 12],
            AccountingToken: [0; 32],
            ApplIdentityData: [b' ' as c_char; 32],
            PutApplType: 0,
            PutApplName: [b' ' as c_char; 28],
            PutDate: [b' ' as c_char; 8],
            PutTime: [b' ' as c_char; 8],
            ApplOriginData: [b' ' as c_char; 4],
        }
    }
}

#[repr(C)]
struct Mqgmo {
    StrucId: [c_char; 4], // "GMO "
    Version: MqLong,      // 1
    Options: MqLong,      // MQGMO_SYNCPOINT | MQGMO_WAIT | MQGMO_FAIL_IF_QUIESCING
    WaitInterval: MqLong, // ms
    Signal1: MqLong,
    Signal2: MqLong,
    ResolvedQName: [c_char; 48],
}
impl Mqgmo {
    fn get_under_syncpoint(wait_ms: MqLong) -> Self {
        Mqgmo {
            StrucId: cc(b"GMO "),
            Version: MQGMO_VERSION_1,
            Options: MQGMO_SYNCPOINT | MQGMO_WAIT | MQGMO_FAIL_IF_QUIESCING,
            WaitInterval: wait_ms,
            Signal1: 0,
            Signal2: 0,
            ResolvedQName: [b' ' as c_char; 48],
        }
    }
}

#[repr(C)]
struct Mqpmo {
    StrucId: [c_char; 4], // "PMO "
    Version: MqLong,      // 1
    Options: MqLong,      // MQPMO_SYNCPOINT | MQPMO_FAIL_IF_QUIESCING
    Timeout: MqLong,
    Context: MqHobj,
    KnownDestCount: MqLong,
    UnknownDestCount: MqLong,
    InvalidDestCount: MqLong,
    ResolvedQName: [c_char; 48],
    ResolvedQMgrName: [c_char; 48],
}
impl Default for Mqpmo {
    fn default() -> Self {
        Mqpmo {
            StrucId: cc(b"PMO "),
            Version: MQPMO_VERSION_1,
            Options: MQPMO_SYNCPOINT | MQPMO_FAIL_IF_QUIESCING | MQPMO_NEW_MSG_ID,
            Timeout: -1,
            Context: 0,
            KnownDestCount: 0,
            UnknownDestCount: 0,
            InvalidDestCount: 0,
            ResolvedQName: [b' ' as c_char; 48],
            ResolvedQMgrName: [b' ' as c_char; 48],
        }
    }
}

// Space-pad a 4-byte struct id into a c_char array.
fn cc(s: &[u8; 4]) -> [c_char; 4] {
    [s[0] as c_char, s[1] as c_char, s[2] as c_char, s[3] as c_char]
}
// An 8-byte format field (MQCHAR8), e.g. MQFMT_STRING.
fn cc8(s: &[u8; 8]) -> [c_char; 8] {
    let mut out = [b' ' as c_char; 8];
    for (i, b) in s.iter().enumerate() {
        out[i] = *b as c_char;
    }
    out
}
// Left-justify + space-pad into a 48-char field (MQI convention).
fn cc48(s: &[u8]) -> [c_char; 48] {
    let mut out = [b' ' as c_char; 48];
    for (i, b) in s.iter().take(48).enumerate() {
        out[i] = *b as c_char;
    }
    out
}
fn set_q_name(dst: &mut [c_char; 48], name: &str) {
    *dst = [b' ' as c_char; 48];
    for (i, b) in name.bytes().take(48).enumerate() {
        dst[i] = b as c_char;
    }
}

// ───────────────────────── FFI: dlopen + MQI ─────────────────────────
extern "C" {
    fn dlopen(filename: *const c_char, flag: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlerror() -> *mut c_char;
}
const RTLD_NOW: c_int = 2;

type FnConnx =
    unsafe extern "C" fn(*const c_char, *mut Mqcno, *mut MqHconn, *mut MqLong, *mut MqLong);
type FnOpen =
    unsafe extern "C" fn(MqHconn, *mut Mqod, MqLong, *mut MqHobj, *mut MqLong, *mut MqLong);
type FnGet = unsafe extern "C" fn(
    MqHconn,
    MqHobj,
    *mut Mqmd,
    *mut Mqgmo,
    MqLong,
    *mut c_void,
    *mut MqLong,
    *mut MqLong,
    *mut MqLong,
);
type FnPut = unsafe extern "C" fn(
    MqHconn,
    MqHobj,
    *mut Mqmd,
    *mut Mqpmo,
    MqLong,
    *mut c_void,
    *mut MqLong,
    *mut MqLong,
);
type FnCmit = unsafe extern "C" fn(MqHconn, *mut MqLong, *mut MqLong);
type FnBack = unsafe extern "C" fn(MqHconn, *mut MqLong, *mut MqLong);
type FnClose = unsafe extern "C" fn(MqHconn, *mut MqHobj, MqLong, *mut MqLong, *mut MqLong);
type FnDisc = unsafe extern "C" fn(*mut MqHconn, *mut MqLong, *mut MqLong);

struct Mqi {
    connx: FnConnx,
    open: FnOpen,
    get: FnGet,
    put: FnPut,
    cmit: FnCmit,
    back: FnBack,
    close: FnClose,
    disc: FnDisc,
}

fn dl_err() -> String {
    unsafe {
        let e = dlerror();
        if e.is_null() {
            "unknown".into()
        } else {
            std::ffi::CStr::from_ptr(e).to_string_lossy().into_owned()
        }
    }
}

impl Mqi {
    fn load() -> Result<Mqi, String> {
        let lib = std::env::var("VEJAS_MQ_LIB").unwrap_or_else(|_| "libmqic_r.so".into());
        let cpath = CString::new(lib.clone()).unwrap();
        let handle = unsafe { dlopen(cpath.as_ptr(), RTLD_NOW) };
        if handle.is_null() {
            return Err(format!("dlopen({lib}) failed: {} — install the IBM MQ client and set VEJAS_MQ_LIB", dl_err()));
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
        Ok(Mqi {
            connx: sym!("MQCONNX", FnConnx),
            open: sym!("MQOPEN", FnOpen),
            get: sym!("MQGET", FnGet),
            put: sym!("MQPUT", FnPut),
            cmit: sym!("MQCMIT", FnCmit),
            back: sym!("MQBACK", FnBack),
            close: sym!("MQCLOSE", FnClose),
            disc: sym!("MQDISC", FnDisc),
        })
    }
}

// ───────────────────────── the Broker seam ─────────────────────────
/// One MQ message crossing the boundary: the body bytes plus the identity fields
/// the sink carries for downstream dedup (ADR-0023 open question 3 — CorrelId).
#[derive(Clone, Debug, Default)]
pub struct MqMessage {
    pub body: Vec<u8>,
    pub correlid: [u8; 24],
}

#[derive(Debug)]
pub struct MqError {
    pub op: &'static str,
    pub comp_code: i32,
    pub reason: i32,
}
impl std::fmt::Display for MqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MQ {} failed: CompCode={} Reason={}", self.op, self.comp_code, self.reason)
    }
}
impl std::error::Error for MqError {}

/// The transactional queue seam. The source/sink loops in `main` are written
/// against this trait so the commit-ordering invariant can be exercised against an
/// in-memory fake with fault injection (the ADR's no-loss claim) without a live
/// queue manager; the real MQI implementation is `MqiQueue`.
pub trait Broker {
    /// MQGET one message under syncpoint, waiting up to `wait`. `Ok(None)` = no
    /// message available within the wait (not an error). The got message is held
    /// under the unit of work until `commit`/`backout`.
    fn get_syncpoint(&mut self, wait: Duration) -> Result<Option<MqMessage>, MqError>;
    /// MQPUT one message under syncpoint (staged in the unit of work).
    fn put_syncpoint(&mut self, msg: &MqMessage) -> Result<(), MqError>;
    /// MQCMIT — make the staged get/put permanent.
    fn commit(&mut self) -> Result<(), MqError>;
    /// MQBACK — roll the unit of work back (a got message returns to the queue).
    fn backout(&mut self) -> Result<(), MqError>;
}

/// The real MQI-backed queue: an open connection + object handle, plus the loaded
/// function pointers. Connects as a client via the standard MQSERVER/CCDT
/// environment (no MQCD struct); the queue manager name may be empty to take the
/// default.
pub struct MqiQueue {
    mqi: Mqi,
    hconn: MqHconn,
    hobj: MqHobj,
    read_buf: Vec<u8>,
}

impl MqiQueue {
    /// Connect to `qmgr` and open `queue` for input (get) or output (put).
    pub fn open(qmgr: &str, queue: &str, for_output: bool) -> Result<MqiQueue, String> {
        let mqi = Mqi::load()?;
        let mut hconn: MqHconn = -1;
        let (mut cc, mut rc): (MqLong, MqLong) = (0, 0);
        let mut cno = Mqcno::default();
        // user/password auth (MQCSP) when VEJAS_MQ_USER/VEJAS_MQ_PASSWORD are set —
        // enterprise QMs run CHCKCLNT(REQUIRED). The buffers and the MQCSP must
        // outlive the MQCONNX call below (they do — same scope).
        let user = std::env::var("VEJAS_MQ_USER").ok().filter(|s| !s.is_empty());
        let pass = std::env::var("VEJAS_MQ_PASSWORD").ok().filter(|s| !s.is_empty());
        // Fixed, zero-padded buffers (MQ_USER_ID / MQ_CSP_PASSWORD max lengths) —
        // like the IBM sample's char[] rather than an exact-length slice. The
        // client's password protection can read the buffer beyond the declared
        // length, so slack + NUL padding matters (an exact-length Vec yields 2139).
        let mut ubuf = [0u8; 64];
        let mut pbuf = [0u8; 256];
        let mut csp = Mqcsp::default();
        if let (Some(u), Some(p)) = (&user, &pass) {
            let ul = u.len().min(ubuf.len());
            let pl = p.len().min(pbuf.len());
            ubuf[..ul].copy_from_slice(&u.as_bytes()[..ul]);
            pbuf[..pl].copy_from_slice(&p.as_bytes()[..pl]);
            csp.AuthenticationType = MQCSP_AUTH_USER_ID_AND_PWD;
            csp.CSPUserIdPtr = ubuf.as_ptr() as *mut c_void;
            csp.CSPUserIdLength = ul as MqLong;
            csp.CSPPasswordPtr = pbuf.as_ptr() as *mut c_void;
            csp.CSPPasswordLength = pl as MqLong;
            cno.Version = MQCNO_VERSION_5;
            cno.SecurityParmsPtr = (&mut csp) as *mut Mqcsp as *mut c_void;
        }
        let mut qmgr_c = [b' ' as c_char; 48];
        for (i, b) in qmgr.bytes().take(48).enumerate() {
            qmgr_c[i] = b as c_char;
        }
        unsafe { (mqi.connx)(qmgr_c.as_ptr(), &mut cno, &mut hconn, &mut cc, &mut rc) };
        if cc == MQCC_FAILED {
            return Err(format!("MQCONNX({qmgr}) failed: CompCode={cc} Reason={rc}"));
        }
        let mut od = Mqod::default();
        set_q_name(&mut od.ObjectName, queue);
        let open_opts = if for_output {
            MQOO_OUTPUT | MQOO_FAIL_IF_QUIESCING
        } else {
            MQOO_INPUT_AS_Q_DEF | MQOO_FAIL_IF_QUIESCING
        };
        let mut hobj: MqHobj = -1;
        unsafe { (mqi.open)(hconn, &mut od, open_opts, &mut hobj, &mut cc, &mut rc) };
        if cc == MQCC_FAILED {
            unsafe { (mqi.disc)(&mut hconn, &mut cc, &mut rc) };
            return Err(format!("MQOPEN({queue}) failed: CompCode={cc} Reason={rc}"));
        }
        Ok(MqiQueue { mqi, hconn, hobj, read_buf: vec![0u8; 4 * 1024 * 1024] })
    }
}

impl Broker for MqiQueue {
    fn get_syncpoint(&mut self, wait: Duration) -> Result<Option<MqMessage>, MqError> {
        let mut md = Mqmd::default();
        let mut gmo = Mqgmo::get_under_syncpoint(wait.as_millis().min(i32::MAX as u128) as MqLong);
        let (mut cc, mut rc, mut data_len): (MqLong, MqLong, MqLong) = (0, 0, 0);
        unsafe {
            (self.mqi.get)(
                self.hconn,
                self.hobj,
                &mut md,
                &mut gmo,
                self.read_buf.len() as MqLong,
                self.read_buf.as_mut_ptr() as *mut c_void,
                &mut data_len,
                &mut cc,
                &mut rc,
            )
        };
        if cc == MQCC_FAILED {
            if rc == MQRC_NO_MSG_AVAILABLE {
                return Ok(None); // idle: no message within the wait, not an error
            }
            return Err(MqError { op: "MQGET", comp_code: cc, reason: rc });
        }
        let n = (data_len as usize).min(self.read_buf.len());
        Ok(Some(MqMessage { body: self.read_buf[..n].to_vec(), correlid: md.CorrelId }))
    }

    fn put_syncpoint(&mut self, msg: &MqMessage) -> Result<(), MqError> {
        let mut md = Mqmd::default();
        md.CorrelId = msg.correlid;
        let mut pmo = Mqpmo::default();
        let (mut cc, mut rc): (MqLong, MqLong) = (0, 0);
        let mut body = msg.body.clone();
        unsafe {
            (self.mqi.put)(
                self.hconn,
                self.hobj,
                &mut md,
                &mut pmo,
                body.len() as MqLong,
                body.as_mut_ptr() as *mut c_void,
                &mut cc,
                &mut rc,
            )
        };
        if cc == MQCC_FAILED {
            return Err(MqError { op: "MQPUT", comp_code: cc, reason: rc });
        }
        Ok(())
    }

    fn commit(&mut self) -> Result<(), MqError> {
        let (mut cc, mut rc): (MqLong, MqLong) = (0, 0);
        unsafe { (self.mqi.cmit)(self.hconn, &mut cc, &mut rc) };
        if cc == MQCC_FAILED {
            return Err(MqError { op: "MQCMIT", comp_code: cc, reason: rc });
        }
        Ok(())
    }

    fn backout(&mut self) -> Result<(), MqError> {
        let (mut cc, mut rc): (MqLong, MqLong) = (0, 0);
        unsafe { (self.mqi.back)(self.hconn, &mut cc, &mut rc) };
        if cc == MQCC_FAILED {
            return Err(MqError { op: "MQBACK", comp_code: cc, reason: rc });
        }
        Ok(())
    }
}

impl Drop for MqiQueue {
    fn drop(&mut self) {
        let (mut cc, mut rc): (MqLong, MqLong) = (0, 0);
        unsafe {
            (self.mqi.close)(self.hconn, &mut self.hobj, MQCO_NONE, &mut cc, &mut rc);
            (self.mqi.disc)(&mut self.hconn, &mut cc, &mut rc);
        }
    }
}

#[cfg(test)]
mod layout {
    use super::*;
    // The MQI v1 descriptor lengths are fixed by cmqc.h (MQ*_LENGTH_1). All fields
    // are 4-byte-or-multiple scalars or byte arrays, so a correct #[repr(C)] has NO
    // padding and must hit these exactly — a mismatch means a layout bug that would
    // corrupt memory against a real queue manager. This is the layout verification
    // the ADR promised, done without a live QM.
    #[test]
    fn descriptor_lengths_match_cmqc() {
        // MQOD/MQMD/MQGMO/MQPMO are the v1 shapes; MQCNO is physically v5 and MQCSP
        // v1 — 64-bit lengths (8-byte MQPTR). A mismatch = a padding/order bug.
        assert_eq!(std::mem::size_of::<Mqod>(), 168, "MQOD_LENGTH_1");
        assert_eq!(std::mem::size_of::<Mqmd>(), 324, "MQMD_LENGTH_1");
        assert_eq!(std::mem::size_of::<Mqgmo>(), 72, "MQGMO_LENGTH_1");
        assert_eq!(std::mem::size_of::<Mqpmo>(), 128, "MQPMO_LENGTH_1");
        assert_eq!(std::mem::size_of::<Mqcno>(), 176, "MQCNO_LENGTH_5 (64-bit)");
        assert_eq!(std::mem::size_of::<Mqcsp>(), 56, "MQCSP_LENGTH_1 (64-bit)");
    }
}

/// Hash an arbitrary idempotency key into the 24-byte CorrelId (ADR-0023 open
/// question 3): MsgId is usually the queue manager's, and CorrelId is 24 binary
/// bytes, so a key is folded to fit. FNV-1a twice over offset domains fills the
/// 24 bytes deterministically — enough for downstream dedup, not a MAC.
pub fn correlid_from_key(key: &str) -> [u8; 24] {
    let mut out = [0u8; 24];
    for (chunk, seed) in out.chunks_mut(8).zip([0xcbf29ce484222325u64, 0x100000001b3, 0x9e3779b97f4a7c15]) {
        let mut h = seed;
        for b in key.bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        chunk.copy_from_slice(&h.to_be_bytes());
    }
    out
}
