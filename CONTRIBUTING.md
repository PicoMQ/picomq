# Contributing

Thank you for your interest in contributing to PicoMQ.

The full contributor guide lives in the docs: [Contribute](https://picomq.com/docs/contribute).

In short: small, focused PRs against `main`, keep the `s3stream/` / `picomq/` boundary intact, and [open an issue](https://github.com/picomq/picomq/issues) first for anything larger than a bug fix.

## Local checks

This repo uses [prek](https://github.com/j178/prek) to run the same fmt/clippy/test checks locally that CI runs. After cloning, install the hooks:

```
prek install --hook-type pre-commit --hook-type pre-push
```

`fmt`, `clippy`, and file-hygiene checks run on every commit; the full test suite runs on `git push`.
