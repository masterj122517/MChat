package com.mcorp.MChat.protocol;

import lombok.Data;


@Data
public class Message {
    private int magicNumber = ProtocolConstants.MAGIC_NUMBER;
    private byte version = ProtocolConstants.VERSION;
    private byte serializer = ProtocolConstants.SERIALIZER_PROTOBUF;
    private byte command;
    private int length;
    private byte[] body; // acutal message content in bytes

}
