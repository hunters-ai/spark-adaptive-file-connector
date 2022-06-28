# How to contribute

The project is conducted via GitHub. Please use the Issues tab to reporting bugs and wanted features. 
If you want to share a new feature with us please raise a PR.

## Testing

We use scalatest for testing. We write unit tests (postfix `Test`) and integration tests (postfix `ITest`).

## Submitting changes

Please send a GitHub Pull Request with a clear list of what you've done 
(read more about [pull requests](http://help.github.com/pull-requests/)). 
The PR should contain the first commit with only test that reproduce the problem (in case of bugfixing). 
Please follow our coding conventions (below).

## Coding conventions

The coding convention is defined in `scalastyle-config.xml`. We use `sbt scalastyle` to check proper formatting of code. 
For automatic formatting we use `scalafmt` with a config defined in `scalafmt.config`. Please use this formatter and 
check style of your code before you submit a PR.

Thanks,
Hunters.ai