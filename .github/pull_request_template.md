## Pull Request submission 

*Insert detailed bullet points about your changes here!*

*Insert any instructions to help the reviewer, e.g. "install new requirements from `environment.yml`"*

*Let the reviewer know what data files are needed (and if applicable, where they are to be found)

#### Closes or fixes

* Detail the ticket(s) you are closing with this PR
Closes #


#### Code

- [ ] **Code runs** The code runs on my machine and/or CDSW
- [ ] **Conflicts resolved** There are no conflicts (I have performed a rebase if necessary)
- [ ] **Requirements** My/our code functions according to the requirements of the ticket
- [ ] **Dependencies** I have updated the environment yaml so it includes any new libraries I have used
- [ ] **Configuration file updated** any high level parameters that the user may interact with have been put into the config file (and imported to the script) 
- [ ] **Clean Code**
    - [ ] Code is as [PEP 8]([url](https://peps.python.org/pep-0008/)) compliant as I can humanly make it
    - [ ] Code passess flake8 linting check
    - [ ] Code adheres to [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself)
- [ ] **Type hints** All new functions have [type hints ](https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html)


#### Documentation

Any new code includes all the following forms of documentation:

- [ ] **Function Documentation** Docstrings within the function(s')/methods have been created
    - [ ] Includes `Args` and `returns` for all major functions 
    - [ ] The docstring details data types
- [ ] **Updated Documentation**: User and/or developer working doc has been updated

#### Data
- [ ] All data needed to run this script is available in Dev/Test
- [ ] All data is excluded from this pull request
- [ ] Secrets checker pre-commit passes

#### Testing
- [ ] **Unit tests** Unit tests have been created and are passing _or a new ticket to create tests has been created_

---

# Peer Review Section

- [ ] All requirements install from (updated) `environment.yaml`
- [ ] Documentation has been created and is clear - **check the working document**
- [ ] Doctrings ([Google format](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)) have been created and accurately describe the function's functionality
- [ ] Unit tests pass, or if not present _a new ticket to create tests has been created_
- [ ] **Code runs** The code runs on reviewer's machine and/or CDSW

#### Final approval (post-review)

The author has responded to my review and made changes to my satisfaction.
- [ ] **I recommend merging this request.**

---

### Review comments

*Insert detailed comments here!*

These might include, but not exclusively:

- bugs that need fixing (does it work as expected? and does it work with other code
  that it is likely to interact with?)
- alternative methods (could it be written more efficiently or with more clarity?)
- documentation improvements (does the documentation reflect how the code actually works?)
- additional tests that should be implemented (do the tests effectively assure that it
  works correctly?)
- code style improvements (could the code be written more clearly?)
- Do the changes represent a change in functionality so the version number should increase? Start a discussion if so.
- As a review you can generates the same outputs from running the code

Your suggestions should be tailored to the code that you are reviewing.
Be critical and clear, but not mean. Ask questions and set actions.
