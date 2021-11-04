## Query Parser

Uses Calcite to parse sql queries into lower level queries. Calcite requires a model to reference to make the parsed queries, so this program pulls from a local postgres database. Currently, only runs sql commands from a file, (with each command having to end with a ; (semicolon)). Let me know if there is anything I should add/modify --Kobi

## Building
- With Maven (Requires Maven (`mvn`) to be installed):
   - Run `mvn package` in the base directory, the file `query_parser-1.jar` should be created in the new `\target` directoy=ry
- Without Maven:
   - There should be a file called `query_parser.jar`, use that

## Running
  - Run from the base directory `java -jar .\target\query_parser-1.jar` if built with maven or `java -jar query_parser.jar` if not.
