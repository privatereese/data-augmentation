# data processing and preparation

This repository is intended to help with the databases of the MaLSAMi project.

The project MaLSAMi, mainly the [client-tools](https://github.com/malsami/client-tools) repository, generates data that is heavily biased towards successfully running task sets.
The Client-tools repository creates single-task task sets at first and recombines the running sets to new two-task task sets.
This method leads to the imbalance of good and bad task sets.
To get it back into balance, we recombine not working task sets and their jobs and mark them as not functional.
These newly created values get written back into the database.

After this step, an almost balanced new database can be used.
The script also suggests a value to generate the perfect, equal amount of task sets. Please always use whole integer values, which should also be higher than suggested to favor a higher amount of data instead of more successfully running tasks.

If you only want to use this feature, please choose "augmentation" as the second parameter.

The script can also calculate minimum, maximum and average task runtimes. These runtimes are appended to the Task Table.

If you want to use this feature, please choose "runtime" as the second parameter.

To use both features, please run the script with "both".

The original database remains unchanged in all cases. A new database is generated every time and the name of the selected modification is appended, e.g. _runtime.db.
The new database is generated next to the old one (same folder).

Usage:

```
python3 main.py <pathToDB> <augmentation/runtime/both>
```
