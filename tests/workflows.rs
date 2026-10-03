// SPDX-License-Identifier: Apache-2.0

//! How workflow inputs reach a shell is a security property, so it is tested
//! like one.
//!
//! The release workflow takes a free-form `version` input and runs in a job
//! holding the Docker Hub token. An expression such as `${{ inputs.version }}`
//! written inside a `run:` script is pasted into the script before the shell
//! parses it, so whoever dispatches the workflow chooses what it executes.
//! These tests keep every expression out of every script, and run the
//! release workflow's tag step to check that the version it does accept, from
//! the environment, is held to a semantic version first.
//!
//! The image build leaves `.github` out of its context, so there the tests
//! find nothing to check and say so; CI, which has the whole checkout, runs
//! them for real.

use std::path::PathBuf;
use std::process::Command;

/// The workflow directory, or `None` where the checkout does not include it.
fn workflows() -> Option<PathBuf> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".github/workflows");
    if dir.is_dir() {
        Some(dir)
    } else {
        eprintln!("  no .github/workflows here (the image build excludes it); nothing to check");
        None
    }
}

/// Leading spaces on a line.
fn indentation(line: &str) -> usize {
    line.len() - line.trim_start_matches(' ').len()
}

/// Every `run:` script in a workflow, with the line it starts on.
///
/// Workflows here write a script either inline after `run:` or as a block
/// scalar indented under it, which is all this needs to read.
fn run_scripts(yaml: &str) -> Vec<(usize, String)> {
    let lines: Vec<&str> = yaml.lines().collect();
    let mut scripts = Vec::new();
    let mut at = 0;
    while at < lines.len() {
        let line = lines[at];
        let key = line.trim_start();
        let key = key.strip_prefix("- ").unwrap_or(key);
        let Some(value) = key.strip_prefix("run:") else {
            at += 1;
            continue;
        };
        let value = value.trim();
        if value.starts_with('|') || value.starts_with('>') {
            let indent = indentation(line);
            let mut body = String::new();
            let mut next = at + 1;
            while next < lines.len()
                && (lines[next].trim().is_empty() || indentation(lines[next]) > indent)
            {
                body.push_str(lines[next]);
                body.push('\n');
                next += 1;
            }
            scripts.push((at + 1, body));
            at = next;
        } else {
            scripts.push((at + 1, value.to_owned()));
            at += 1;
        }
    }
    scripts
}

/// The `run:` script of the step named `name` in `yaml`.
fn step_script(yaml: &str, name: &str) -> String {
    let start = yaml
        .find(&format!("name: {name}\n"))
        .unwrap_or_else(|| panic!("no step named {name:?}"));
    run_scripts(&yaml[start..])
        .into_iter()
        .next()
        .map(|(_, script)| script)
        .unwrap_or_else(|| panic!("the step {name:?} has no `run:` script"))
}

#[test]
fn no_workflow_expands_an_expression_inside_a_shell_script() {
    let Some(dir) = workflows() else {
        return;
    };
    let mut checked = 0;
    for entry in std::fs::read_dir(dir).expect("read the workflow directory") {
        let path = entry.expect("a directory entry").path();
        if !matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("yml" | "yaml")
        ) {
            continue;
        }
        let yaml = std::fs::read_to_string(&path).expect("read a workflow");
        for (line, script) in run_scripts(&yaml) {
            checked += 1;
            assert!(
                !script.contains("${{"),
                "{}:{line} expands an expression inside a `run:` script, where it becomes \
                 code; pass it through `env:` and refer to the variable instead",
                path.display()
            );
        }
    }
    assert!(checked > 0, "found no `run:` scripts to check");
}

#[test]
fn the_release_version_has_to_be_a_semantic_version_before_it_becomes_a_tag() {
    let Some(dir) = workflows() else {
        return;
    };
    if Command::new("bash").arg("--version").output().is_err() {
        eprintln!("  no bash here; nothing to run the tag step with");
        return;
    }
    let yaml = std::fs::read_to_string(dir.join("publish.yml")).expect("read publish.yml");
    let script = step_script(&yaml, "Compute tags");

    // The step's outputs, or `None` if it refused the version. The step
    // appends its outputs to the file `GITHUB_OUTPUT` names, which here is
    // the captured stdout.
    let run = |version: &str| {
        let output = Command::new("bash")
            .arg("-c")
            .arg(&script)
            .env("VERSION", version)
            .env("GITHUB_OUTPUT", "/dev/stdout")
            .output()
            .expect("run the tag step");
        output
            .status
            .success()
            .then(|| String::from_utf8(output.stdout).expect("UTF-8 outputs"))
    };

    assert_eq!(
        run("").as_deref(),
        Some(
            "svc=docker.io/pipestreamai/grpc-lol-html:latest\n\
             ui=docker.io/pipestreamai/grpc-lol-html-ui:latest\n"
        ),
        "no version means :latest alone"
    );
    let tagged = run("0.1.0").expect("a plain semantic version is accepted");
    assert!(tagged.contains(
        "docker.io/pipestreamai/grpc-lol-html:latest,docker.io/pipestreamai/grpc-lol-html:0.1.0\n"
    ));
    assert!(tagged.contains("docker.io/pipestreamai/grpc-lol-html-ui:latest,docker.io/pipestreamai/grpc-lol-html-ui:0.1.0\n"));
    assert!(run("1.2.3-rc.1").is_some(), "a pre-release is accepted");

    for refused in [
        "v1.0.0",
        "1.0",
        "01.0.0",
        "1.0.0+build.5",
        "1.0.0 latest",
        "1.0.0\nlatest",
        "1.0.0;touch /tmp/pwned",
        "$(id)",
        "`id`",
        "1.0.0,docker.io/attacker/image:x",
    ] {
        assert_eq!(run(refused), None, "{refused:?} should be refused");
    }
}
