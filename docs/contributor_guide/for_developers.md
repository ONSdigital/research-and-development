# Contributing

We are not looking for active developers on this project outside of ONS. However we love contributions in the form of feedback and constructive criticism of our code! We've compiled this documentation to help you understand our
contributing guidelines. [If you still have questions, please contact us][email] and
we'd be happy to help!

## Code of Conduct

[Please read `CODE_OF_CONDUCT.md` before contributing][code-of-conduct].

## Getting started

To start contributing, open your terminal, and install the required Python packages,
and [pre-commit hooks][pre-commit] using:

```shell
pip install -r requirements.txt
pre-commit install
```

or the `make` command:

```shell
make requirements
```

The pre-commit hooks are a security feature to ensure, for example, no secrets[^1],
large data files, and Jupyter notebook outputs are accidentally committed into the
repository. [For more information on pre-commit hooks see our
documentation][docs-pre-commit-hooks].

[^1]: [Only secrets of specific patterns are detected by the pre-commit
      hooks][docs-pre-commit-hooks-secrets-definition].

## For developers

The Research and Development project is being developed in the open in line with recommendations in the [Government Service Manual](https://www.gov.uk/service-manual/service-standard/point-12-make-new-source-code-open) that state that new code should be open. Hosting the code on GitHub is an easy way to adhere to this.

## Contributing
We are not looking for developers external to ONS, but anybody may scrutinise the code and give feedback.

[If you want to help us build, and improve `research-and-development`, view our
contributing guidelines][contributing].

## Developer set up

To get developers set up, we have created this guide to make setting up your working environment easier.

### Using VS Code

We strongly suggest that developers on the team use VS Code for programming as it is an industry-standard tool and we are more likely to be able to support it if you run into problems, compared to an IDE we are not familiar with.

#### Downloading VS Code

You can download VS code from the [VS code download page](https://code.visualstudio.com/download). Select the correct installer for your OS and follow the instructions.

## Conda

On the project team, virtual environments (VE) will be used. Conda is our chosen VE manager. You will need to make a service desk request to get the Anaconda suite installed.

## Recreating the environment and installing requirements

Once you have cloned this repository, you will have acces to a file called `environment.yml`. You will use conda to recreate the environment with all the dependencies you need for this project using this command:

`conda env create -f environment.yml -v`

This should create and environment for you with Python 3.10+ installed and every other dependency of the project so far.

## Secrets

A `.secrets` file with the [required secrets and
  credentials](#required-secrets-and-credentials)
- [load environment variables][docs-loading-environment-variables] from `.env`

To install the Python requirements, open your terminal and enter:

```shell
pip install -r requirements.txt
```

### Update the environment yaml

If you have installed a new package as part of your development work, then the environment yaml needs to be updated, so the environment can be shared with the wider team.

To do so:
```
conda env export > environment.yml
```
You will need to add/commit/push the changes to the environment yaml so they get pushed up to the repository and others can run your code.

If the installation of additional libraries are needed to run your code, you must make a note of this in the notes or comments of the Pull Request so the reviewer can update their environment.


## Git and Github Setup

You will need to follow instructions to set up your git configuration to work with Github.

### GitHub and security considerations

This project's GitHub repository is *public*, which means that anyone can view them. As such it is very important that certain information is not made publicly available. This includes passwords (even if hashed), data, and any code that contains sensitive information or methods, e.g. disclosure parameters.

The [QA of Code for Analysis and Research](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html) contains a [comprehensive list](https://best-practice-and-impact.github.io/qa-of-code-guidance/version_control.html#what-should-i-version-control) of what not to include in a public repository and methods to [avoid committing sensitive information to Git repositories](https://best-practice-and-impact.github.io/qa-of-code-guidance/version_control.html#avoid-commiting-sensitive-information-to-git-repositories), e.g. [`.gitignore`](https://best-practice-and-impact.github.io/qa-of-code-guidance/version_control.html#gitignore-files).

Ensure that if you are using a `.gitconfig` file to store your token that the CDSW project is within your own context rather than shared and is set to *private*. The `.gitconfig` file should **not** be pushed to GitHub as that will compromise the security of your account. If you use one GitHub repository per CDSW project and clone directly through the CDSW UI when creating the project then ensure `.gitconfig` is added to `.gitignore`; this should not be an issue if your project structure is `~/repo`, which is the default when using `git clone` and working with several repositories per CDSW project.


## Code conventions

[We mainly follow the GDS Way in our code conventions][gds-way].

### Python

For Python code, [we follow the GDS Way Python style guide][gds-way-python] with a line
length of 88; the flake8 pre-commit hook should help with this!

### Markdown

Local links can be written as normal, but external links should be referenced at the
bottom of the Markdown file for clarity. For example:

Use a [local link to reference the `README.md`](../../README.md) file, but [an external
link for GOV.UK][gov-uk].

We also try to wrap Markdown to a line length of 88 characters, but this is not
strictly enforced in all cases, for example with long hyperlinks.


### Git and GitHub

We use Git to version control the source code. [Please read the GDS Way for details on
Git best practice][gds-way-git]. This includes how to write good commit messages, use
`git rebase` for local branches and `git merge --no-ff` for merges, as well as using
`git push --force-with-lease` instead of `git push -f`.

[If you want to modify the `.gitignore` files, see the template
documentation][docs-updating-gitignore] for further details.

Our source code is stored on GitHub. Pull requests into `main` require at least one
approved review.

## Testing

[Tests are written using the `pytest` framework][pytest], with its configuration in the
`pyproject.toml` file. Note, only tests in the `tests` folder are run. To run the
tests, enter the following command in your terminal:

```shell
pytest
```

### Code coverage

[Code coverage of Python scripts is measured using the `coverage` Python
package][coverage]; its configuration can be found in `pyproject.toml`. Note coverage
only extends to Python scripts in the `src` folder.

To run code coverage, and view it as an HTML report, enter the following command in
your terminal:

```shell
coverage run -m pytest
coverage html
```

or use the `make` command:

```shell
make coverage_html
```

The HTML report can be accessed at `htmlcov/index.html`.

## Documentation

[We write our documentation in MyST Markdown for use in Sphinx][myst]. This is mainly
stored in the `docs` folder, unless it's more appropriate to store it elsewhere, like
this file.

[Please read our guidance on how to write accessible
documentation][docs-write-accessible-documentation], as well as our [guidance on
writing Sphinx documentation][docs-write-sphinx-documentation]. This allows you to
build the documentation into an accessible, searchable website.

[code-of-conduct]: ./CODE_OF_CONDUCT.md
[coverage]: https://coverage.readthedocs.io/
[docs-pre-commit-hooks]: ./pre_commit_hooks.md
[docs-pre-commit-hooks-secrets-definition]: ./pre_commit_hooks.md#definition-of-a-secret-according-to-detect-secrets
[docs-updating-gitignore]: ./updating_gitignore.md
[docs-write-accessible-documentation]: ./writing_accessible_documentation.md
[docs-write-sphinx-documentation]: ./writing_sphinx_documentation.md
[gds-way]: https://gds-way.cloudapps.digital/
[gds-way-git]: https://gds-way.cloudapps.digital/standards/source-code.html
[gds-way-python]: https://gds-way.cloudapps.digital/manuals/programming-languages/python/python.html#python-style-guide
[myst]: https://myst-parser.readthedocs.io/
[pre-commit]: https://pre-commit.com
[pytest]: https://docs.pytest.org/
[gov-uk]: https://www.gov.uk/
[email]: mailto:james.westwood@ons.gov.uk
