# Virtual Environments

On this project we isolate our dependencies by using virtual environments (VEs) using our chosen VE manager, Conda.

#### Install Anaconda

Note: these are ONS-specific instructions

Anaconda should be installed via a service desk request. To verify it has been installed correctly, run:
`conda --version`

#### Create environment

We will use Python version 3.10 for this project - unless we decide to change it later. In the root directory of this project there is a file, `environment.yml` from which we can create the Python 3.10 environment.

Use:

`conda env create --file environment.yml`

This should create an environment for you called `resdev`.

After runnning the line, verify the environment has been created, run: `conda env list` to see if your new environment is listed.

If there are any errors, contact the project tech lead.


#### Activate environment

Then every time you want to develop code for the R&D project you should activate the environment

`$ conda activate resdev`

Then you should see the environment name in brackets before the prompt, similar to:

`(resdev) $`


#### Update the environment yaml

If you have installed a new package as part of your development work, then the environment yaml needs to be updated, so the environment can be shared with the wider team. You will need to add/commit/push the changes to the environment yaml so they get pushed up to the repository and others can run your code.

To do so:

```
conda env export > environment.yml
```

### WARNING: This will export a section in the `environment.yml` called `channels` which contains your Artifactory password. Delete this entire section.

Then do the normal:
```
git add environment.yml
git commit -m "REASON FOR UPDATING ENVIRONMENT"
git push
```

### Note: If the installation of additional libraries are needed to run your code, you may want to note this in the notes or comments of the Pull Request.

## Updating an environment

You can update the environment by first activating the environment with `conda activate resdev` and then update it with `conda env update`.

Alternatively from outside the environment:
```
conda env update --file environment.yml --name resdev
```
