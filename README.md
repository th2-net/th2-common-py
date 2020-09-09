# th2-common-python

## Installing
Use the following command to configure the required packages:
```
pip install th2-common==version -i https://nexus.exactpro.com/repository/th2-pypi/simple/ --extra-index-url https://pypi.python.org/simple/
```
If you already have the kernel installed, use the following command to update:
```
pip install th2-common==version -i https://nexus.exactpro.com/repository/th2-pypi/simple/ --extra-index-url https://pypi.python.org/simple/ -U
```
Where `version` is the tag number in the repository.

Or just add it as a dependency to the requirements.txt. Example:

```th2-common==1.1.63```