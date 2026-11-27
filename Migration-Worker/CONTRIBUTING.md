# Contributing to Container Migration Worker

Firstly, thank you for considering contributing to the Container Migration Worker!
It's people like you that make the open-source community such a great place to be creative,
learn, and inspire others. Here is a set of guidelines for contributing to this project.
These are mostly guidelines, not rules, so use your best judgment and feel free to propose
changes to this document in a pull request.

## Getting Started

Contributions are made to this repo via Issues and Pull Requests (PRs). A few general guidelines
that cover both:

- Search for existing Issues and PRs before creating your own.
- We work hard to make sure issues are handled in a timely manner but, depending on the impact,
  it could take a while to investigate the root cause. A friendly ping in the comment thread to the
  submitter or a contributor can help draw attention if your issue is urgent.

### Issues

Issues should be used to report problems with the worker, request a new feature, or to
discuss potential changes before a PR is created. When you create a new Issue, a template will
be provided for you to fill out which will help the maintainers understand your issue.

### Pull Requests

PRs to our project are always welcome and can be a quick way to get your fix or improvement
slated for the next release. In general, PRs should:

- Only fix/add the functionality in question OR address wide-spread whitespace/style issues, not both.
- Add unit or integration tests for fixed or changed functionality (if a test suite already exists).
- Address a single concern in the least number of changed lines as possible.
- Include documentation in the repo or on our docs site.
- Be accompanied by a complete Pull Request template (loaded automatically when a PR is created).

## Development

For changes to the codebase and documentation, it's best to fork the repo, and then submit a pull
request from your fork back to the main repo.

A few conventions to follow when working on the codebase:

- Go code comments are written in English.
- Every Go source file starts with the Apache License header.
- Generated files (`*.pb.go` under `pkg/apis/`) are produced by `make grpc-proto` from
  `pkg/apis/container_state_migration.proto` and `pkg/apis/xdm/xdm_checkpoint.proto`;
  do not edit them by hand - change the proto file and regenerate.
- The repo does not commit `vendor/`; use `go mod tidy` to refresh dependencies.
- The worker is a Linux daemon: `pkg/util/archive` contains Linux-only symbols, so
  builds and vet runs on a non-Linux host must set `GOOS=linux` (the Makefile's
  `build` target already does).

## Testing

Please make sure to run the tests before submitting a pull request and include any new tests that
cover your changes. This helps to ensure the quality of the codebase and functionality.

```sh
make fmt vet
GOOS=linux GOARCH=amd64 go test ./...
```

## Submitting Changes

After getting some feedback, push to your branch:

```sh
git push origin my-branch
```

Submit a pull request to the repository in the correct branch. Include the relevant issue number
if applicable.

## Review Process

The core team looks at Pull Requests on a regular basis. After feedback has been given, we expect
responses within two weeks. After two weeks, we may close the PR if it isn't showing any activity.

## Community

Discussions about the Container Migration Worker take place on this repository's Issues and
Pull Requests sections. Anybody is welcome to join these conversations.

Wherever possible, we use public discussions for the benefit of the community. If you have questions
or want to discuss a topic in private, please contact the maintainers.

---

Thanks again for your contribution, we're looking forward to seeing what you build with the
Container Migration Worker!
