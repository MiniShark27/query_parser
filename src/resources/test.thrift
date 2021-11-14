//Generates the ThriftService.java file in the thrift_server packege
//and will be used to generate the .cpp and .h files later

namespace java thrift_server  // defines the namespace    
      
service ThriftService {  // defines the service to add two numbers  
  string test(1:string n1, 2:string n2), //defines a method  
} 