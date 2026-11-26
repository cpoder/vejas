//! Child processes: a shell command that inherits none of the runtime's own
//! credentials.

/// Is this variable a credential of the runtime itself?
fn is_runtime_credential(name: &str) -> bool {
    matches!(name, "VEJAS_TOKEN" | "VEJAS_APPROVAL_TOKEN" | "VAULT_TOKEN")
        || name.starts_with("VEJAS_SECRET_")
}

/// Keep the runtime's own credentials out of a child's environment. A child gets
/// what its manifest declares in `ENV`, and an agent CLI what it needs, not the
/// approval token or every secret by inheritance, where an `env` in a log or an
/// agent reading its environment sends them on. This is hygiene and not a
/// sandbox: a child runs as the same user and reaches the same bus.
pub fn without_runtime_credentials(cmd: &mut std::process::Command) {
    for (name, _) in std::env::vars_os() {
        if name.to_str().is_some_and(is_runtime_credential) {
            cmd.env_remove(name);
        }
    }
}

/// `sh -c <cmd>` for an exec driver.
pub(super) fn shell(cmd: &str) -> std::process::Command {
    let mut child = std::process::Command::new("sh");
    child.arg("-c").arg(cmd);
    without_runtime_credentials(&mut child);
    child
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_child_inherits_no_runtime_credential() {
        std::env::set_var("VEJAS_APPROVAL_TOKEN", "the-approval-token");
        std::env::set_var("VEJAS_SECRET_DEMO_KEY", "a-secret");
        std::env::set_var("VEJAS_UNRELATED", "kept");
        let out = shell("env").env("DECLARED", "from-the-manifest").output().unwrap();
        let env = String::from_utf8_lossy(&out.stdout);
        assert!(!env.contains("the-approval-token") && !env.contains("a-secret"), "{env}");
        assert!(env.contains("VEJAS_UNRELATED=kept"), "{env}");
        assert!(env.contains("DECLARED=from-the-manifest"), "{env}");
    }
}
