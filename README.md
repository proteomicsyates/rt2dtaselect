# rt2dtaselect
Adds retention time to dtaselect output files, by reading it from ms2 (either in the local system or in a remote server).

### Download latest release [here](https://github.com/proteomicsyates/rt2dtaselect/releases/latest)

Usage:  
```
java -jar rt2dtaselect input_folder user_name password  
```
where: 
* ```input_folder```: is the folder where the DTASelect output files are located.
* ```user_name```: user name to access the server where the ms2 file is located.
* ```password```: password to access the server where the ms2 file is located.
