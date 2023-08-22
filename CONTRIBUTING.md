# Contributing

We warmly welcome and greatly appreciate contributions from the
community. By participating you agree to the [code of
conduct](https://github.com/greenplum-db/gpbackup/blob/main/CODE-OF-CONDUCT.md).
Overall, we follow GPDB's comprehensive contribution policy. Please
refer to it [here](https://github.com/greenplum-db/gpdb#contributing)
for details.

## Getting Started

* Sign our [Contributor License Agreement](https://cla.vmware.com/cla/1/preview)
* Fork the gpbackup repository on GitHub
* Run `go get github.com/greenplum-db/gpbackup/...` and add your fork as a remote
* Run `make depend` to install required dependencies
* Follow the README to set up your environment and run the tests

## Creating a change

* Create your own feature branch (e.g. `git checkout -b
  gpbackup_branch`) and make changes on this branch.
* Try and follow similar coding styles as found throughout the code
  base.
* Make commits as logical units for ease of reviewing.
* Rebase with main often to stay in sync with upstream.
* Add new tests to cover your code. We use
  [Ginkgo](http://onsi.github.io/ginkgo/) and
  [Gomega](https://onsi.github.io/gomega/) for testing.
* Ensure a well written commit message as explained
  [here](https://chris.beams.io/posts/git-commit/).
* Run `make format`, `make test`, and `make end_to_end` in your
  feature branch and ensure they are successful.
* Push your local branch to the fork (e.g. `git push <your_fork>
  gpbackup_branch`)

## Submitting a Pull Request

* Create a [pull request from your
  fork](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork).
* Address PR feedback with fixup and/or squash commits:
```
git add .
git commit --fixup <commit SHA>
  -- or --
git commit --squash <commit SHA>
```
* Once approved, before merging into main squash your fixups with:
```
git rebase -i --autosquash origin/main
git push --force-with-lease $USER <my-feature-branch>
```

Your contribution will be analyzed for product fit and engineering
quality prior to merging. Your pull request is much more likely to be
accepted if it is small and focused with a clear message that conveys
the intent of your change.

## Community

Connect with Greenplum on:
* [Slack](https://greenplum.slack.com/)
* [Dev Google Group mailing list](https://groups.google.com/a/greenplum.org/forum/#!forum/gpdb-dev/join)
