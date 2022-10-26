# Research and Development Project

Calculating national and regional research and development expenditure as part of [national accounts](https://www.ons.gov.uk/economy/nationalaccounts).

Additional information about the aims and objectives of the project will go here when it is available. The project is currently in pre-discovery.







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


## Acknowledgements

[This project structure is based on the `govcookiecutter` template
project][govcookiecutter]. Guidance on using the govcookiecutter can be found on [this youtube video](https://www.youtube.com/watch?v=N7_d3k3uQ_M) and in the [documentation here](https://dataingovernment.blog.gov.uk/2021/07/20/govcookiecutter-a-template-for-data-science-projects/).

The text in the "For Developers" section was adapted from the README of the [Transport Efficiency Project](https://github.com/jwestw/Public_Transport_Efficiency) which was mostly written by Chloe Murrell.

Some of the text, especially that covering git configuration and security considerations was adapted from work by David Foster and Rowan Hemsi at ONS.

[contributing]: ./docs/contributor_guide/CONTRIBUTING.md
[govcookiecutter]: https://github.com/best-practice-and-impact/govcookiecutter
[docs-loading-environment-variables]: ./docs/user_guide/loading_environment_variables.md
[docs-loading-environment-variables-secrets]: ./docs/user_guide/loading_environment_variables.md#storing-secrets-and-credentials
