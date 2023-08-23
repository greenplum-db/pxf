# Contributing

We warmly welcome and appreciate contributions from the community!
By participating you agree to the [code of conduct](https://github.com/greenplum-db/pxf/blob/main/CODE-OF-CONDUCT.md).
To contribute:

- Sign our [Contributor License Agreement](https://cla.vmware.com/cla/1/preview).

- Fork the PXF repository on GitHub.

- Clone the repository.

- Follow the README.md to set up your environment and run the tests.

- Create a change

    - Create a topic branch.

    - Make commits as logical units for ease of reviewing.

    - Follow similar coding styles as found throughout the code base.

    - Rebase with main often to stay in sync with upstream.

    - Add appropriate unit and automation tests.

    - Ensure a well written commit message as explained [here](https://chris.beams.io/posts/git-commit/) and [here](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).

- Submit a pull request (PR).

    - Create a [pull request from your fork](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/.creating-a-pull-request-from-a-fork).

    - Address PR feedback with fixup and/or squash commits.
        ```
        git add .
        git commit --fixup <commit SHA> 
            Or
        git commit --squash <commit SHA>
        ```    

    - Once the PR is approved, project committers will merge it to main
      branch according to the product release schedule. They might further
      squash the commits in the PR if they deem necessary.

# Community

Connect with PXF and Greenplum on:
- [Slack](https://greenplum.slack.com/)
- [Dev Google Group mailing list](https://groups.google.com/a/greenplum.org/forum/#!forum/gpdb-dev/join)
