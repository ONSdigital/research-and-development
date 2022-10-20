# Research and Development Project

Calculating national and regional research and development expenditure as part of national accounts.


## For developers

The Research and Development project is being developed in the open in line with recommendations in the [Government Service Manual](https://www.gov.uk/service-manual/service-standard/point-12-make-new-source-code-open) that state that new code should be open. Hosting the code on GitHub is an easy way to adhere to this.

## Contributing
We are not looking for developers external to ONS, but anybody may scrutinise the code and give feedback.

## Developer set up

To get developers set up, we have created this guide to make setting up your working environment easier.

### Using VS Code

We strongly suggest that developers on teh team use VS Code for programming as it is an industry-standard tool, and we are more like to be able to support it if you run into problems, rather than an IDE we are not familiar with.

#### Downloading VS Code

You can download VS code from internet [VS code download link](https://code.visualstudio.com/download). Select the correct installer for your OS and follow the instructions.

## Conda

On the project team, virtual environments (VE) will be used. Conda is our chosen VE manager. You will need to make a service desk request to get the Anaconda suite installed.

## Recreating the environment

Once you have cloned this repository, you will have acces to a file called `environment.yml`. You will use conda to recreate the environment with all the dependencies you need for this project using this command:

`conda env create -f environment.yml -v`

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



### Requirements

[```Contributors have some additional requirements!```][contributing]

- Python 3.10+ installed
- a `.secrets` file with the [required secrets and
  credentials](#required-secrets-and-credentials)
- [load environment variables][docs-loading-environment-variables] from `.env`

To install the Python requirements, open your terminal and enter:

```shell
pip install -r requirements.txt
```

## Required secrets and credentials

To run this project, [you need a `.secrets` file with secrets/credentials as
environmental variables][docs-loading-environment-variables-secrets]. The
secrets/credentials should have the following environment variable name(s):

| Secret/credential | Environment variable name | Description                                |
|-------------------|---------------------------|--------------------------------------------|
| Secret 1          | `SECRET_VARIABLE_1`       | Plain English description of Secret 1.     |
| Credential 1      | `CREDENTIAL_VARIABLE_1`   | Plain English description of Credential 1. |

Once you've added, [load these environment variables using
`.env`][docs-loading-environment-variables].

## Licence

Unless stated otherwise, the codebase is released under the MIT License. This covers
both the codebase and any sample code in the documentation. The documentation is ©
Crown copyright and available under the terms of the Open Government 3.0 licence.

## Contributing

[If you want to help us build, and improve `research-and-development`, view our
contributing guidelines][contributing].

## Acknowledgements

[This project structure is based on the `govcookiecutter` template
project][govcookiecutter].

The text in the "For Developers" section was adapted from the README of the [Transport Efficiency Project](https://github.com/jwestw/Public_Transport_Efficiency) which was mostly written by Chloe Murrell.

Some of the text, especially that covering git configuration and security considerations was adapted from work by David Foster and Rowan Hemsi at ONS.

[contributing]: ./docs/contributor_guide/CONTRIBUTING.md
[govcookiecutter]: https://github.com/best-practice-and-impact/govcookiecutter
[docs-loading-environment-variables]: ./docs/user_guide/loading_environment_variables.md
[docs-loading-environment-variables-secrets]: ./docs/user_guide/loading_environment_variables.md#storing-secrets-and-credentials
