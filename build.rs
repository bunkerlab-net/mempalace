fn main() {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .expect("failed to run git");

    assert!(output.status.success(), "git rev-parse failed");

    let sha = String::from_utf8_lossy(&output.stdout);
    println!("cargo:rustc-env=GIT_SHORT_SHA={}", sha.trim());
    println!("cargo:rerun-if-changed=.git/HEAD");
}
