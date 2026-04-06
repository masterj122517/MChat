package com.mcorp.MChat.protocol;


// Store protocol constants here
public interface ProtocolConstants {

     int MAGIC_NUMBER = 0x4D434854; // "MCHT" in ASCII
     byte VERSION = 1;

    // Command types
     byte COMMAND_LOGIN     = 1;
     byte COMMAND_CHAT      = 2;
     byte COMMAND_HEARTBEAT = 3;
     byte COMMAND_ACK       = 4;  // ACK acknowledgement
     byte COMMAND_GROUP_OP  = 5;  // Group operations (create/join/leave)
     byte COMMAND_LOGIN_RESP = 6; // Login response

    // Serializer types: 0 for JSON, 1 for Protobuf
    byte SERIALIZER_PROTOBUF = 1;
}
