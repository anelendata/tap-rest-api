# Contributing

Contributions are welcome, and they are greatly appreciated! Every little bit
helps, and credit will always be given.

You can contribute in many ways:

## Types of Contributions

### Report Bugs

Report bugs at https://github.com/anelendata/tap_rest_api

If you are reporting a bug, please include:

- Your operating system name and version.
- Any details about your local setup that might be helpful in troubleshooting.
- Detailed steps to reproduce the bug.

### Fix Bugs

Look through the GitHub issues for bugs. Anything tagged with "bug" and "help
wanted" is open to whoever wants to implement it.

### Implement Features

Look through the GitHub issues for features. Anything tagged with "enhancement"
and "help wanted" is open to whoever wants to implement it.

### Write Documentation

tap_rest_api could always use more documentation, whether as part of the
official README.md, in docstrings, or even on the web in blog posts,
articles, and such.

### Submit Feedback

The best way to send feedback is to file an issue at https://github.com/anelendata/tap_rest_api/issues.

If you are proposing a feature:

- Explain in detail how it would work.
- Keep the scope as narrow as possible, to make it easier to implement.
- Remember that this is a volunteer-driven project, and that contributions
  are welcome :)

## Submit a pull request through the GitHub website.

### Pull Request Guidelines

Before you submit a pull request, check that it meets these guidelines:

1. The pull request should include tests.
2. If the pull request adds functionality, the docs should be updated. Put
   your new functionality into a function with a docstring, and add the
   feature to the list in README.md.

3. The pull request should pass the `tests` GitHub Actions workflow
   (`pytest tests/unit` on Python 3.9-3.12). Run it locally with
   `uv run --project . pytest -q tests/unit`.

## Releasing (maintainers)

Releases publish to PyPI as [`tap-rest-api`](https://pypi.org/project/tap-rest-api/)
via GitHub Actions **Trusted Publishing** (OIDC) — no API token is stored in the
repo. To cut a release:

1. Add a new section at the top of [`HISTORY.md`](HISTORY.md) for the version.
2. Bump `version` in `pyproject.toml`.
3. Commit both.
4. Tag and push:

   ```bash
   git push origin master
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```

   The `v*` tag triggers `.github/workflows/release.yml`: build -> smoke
   (install the wheel and the sdist in a clean venv, check the version and the
   bundled `default_spec.json`, run `tests/unit` against the installed package)
   -> publish -> verify (install the published version from PyPI). Approve the
   `pypi` environment deployment if GitHub prompts.
5. Optionally create the GitHub release from the tag:
   `gh release create vX.Y.Z --repo anelendata/tap-rest-api --title vX.Y.Z --notes "..."`.

Notes:

- **PyPI versions are immutable.** A version can never be re-uploaded, so a
  broken build means bumping to the next patch — never reusing a number.
- Keep the tag and `pyproject.toml` `version` in sync (`vX.Y.Z` <-> `X.Y.Z`); the
  build job fails when they differ.
- The README renders as the PyPI description; it only updates on a new release.

One-time setup (already needed before the first automated release):

- On PyPI, project `tap-rest-api` -> Settings -> Publishing -> add a GitHub
  trusted publisher: owner `anelendata`, repository `tap-rest-api`, workflow
  `release.yml`, environment `pypi`.
- On GitHub, repository Settings -> Environments -> create `pypi` (optionally with
  required reviewers so each publish needs an approval).
