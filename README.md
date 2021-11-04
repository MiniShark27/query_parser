## Query Parser

Uses Calcite to parse sql queries into lower level queries. Calcite requires a model to reference to make the parsed queries, so this program pulls from a local postgres database. Currently, only runs sql commands from a file, (with each command having to end with a ; (semicolon)). Let me know if there is anything I should add/modify --Kobi

## Building
Run the following:
```bash
git clone https://github.com/MiniShark27/query_parser.git
cd .\query_parser\
./mvnw clean compile assembly:single
```
Note, if you get an error saying `JAVA_HOME` is not set, either java is not installed or the `JAVA_HOME` enviroment variable is not set. For convenience, [here](https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.1+12/OpenJDK17U-jdk_x64_windows_hotspot_17.0.1_12.msi) is a link to download Java, click the option to set `JAVA_HOME` if prompted.

Backup link: https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.1+12/OpenJDK17U-jdk_x64_windows_hotspot_17.0.1_12.msi

## Running
Run the following
```bash
java -jar .\target\query_parser-1-jar-with-dependencies.jar
```
It will prompt for the following (to connect to a local postgres database)
1. `Please input the name of the maintenence database:`
   - input the name of the database that it will connect to
2. `Please input the port where the database is:`
   - The port that the local database (outlined before) is hosted on
3. `Please input the username for the database:`
   - The username of a user who has Read permissions for the database (outlined before)
4. `Please input the password for the database:`
   - The password for the user previously specified
   
Then It will repeatedly prompt for:
1. `Enter File with query(s) to execute (type quit to quit):`
   - This is asking for a file (relative to the directory where the program was executed). An example file is `test.sql`. Typing `quit` will exit the program at this time.
