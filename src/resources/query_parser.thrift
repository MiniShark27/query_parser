//Generates the ThriftService.java file in the thrift_server packege
//and will be used to generate the .cpp and .h files later

namespace java thrift_server  // defines the namespace  

//Used to represent an exception in java
exception JavaException {
    1: string exceptionType;
    2: string reason;
}
      
service ThriftService {  // defines the service
  //Used to create a query parser connected to a database
  //Returns the uuid as a string corresponding to the QueryParser (needed for runQuery) 
  string createInstance(1:string dbUrl, 2:string user, 3:string password, 4:string name) throws (1:JavaException e),
  //Used to run a query parser that has been created
  //Returns the output object as a string
  string runQuery(1:string id, 2:string query) throws (1:JavaException e)
} 