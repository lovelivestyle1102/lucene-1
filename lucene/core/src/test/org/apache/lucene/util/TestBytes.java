package org.apache.lucene.util;

import org.junit.Test;

import java.io.IOException;

public class TestBytes {
    @Test
    public void testInt(){
        byte[] bytes = writeVInt(3);
        printBytes(bytes);
        int number = readVInt(bytes);
        System.out.println(number);
        bytes = writeVInt(128);
        printBytes(bytes);
        number = readVInt(bytes);
        System.out.println(number);
        bytes = writeVInt(129);
        printBytes(bytes);
        number = readVInt(bytes);
        System.out.println(number);
        bytes = writeVInt(256);
        printBytes(bytes);
        number = readVInt(bytes);
        System.out.println(number);
        bytes = writeVInt(512);
        printBytes(bytes);
        number = readVInt(bytes);
        System.out.println(number);
    }

    public void printBytes(byte[] bytes){
        for (int i = 0; i < bytes.length; i++) {
            System.out.print(bytes[i]+"   ");
        }
        System.out.println();
    }


    public byte[] writeVInt(int i){
        int bytesRequired = bytesRequired(i);

        byte[] res = new byte[bytesRequired];

        //0x7f 0111 1111    ~ ->  1000 0000
        //0x80 1000 0000
        int j = 0;

        while ((i & ~0x7F) != 0) {
            //或上0x80是相当于加上-
            res[j++] = ((byte) ((i & 0x7F) | 0x80));

            //高位向下移
            i >>>= 7;
        }

        //如果最后高位有值则记录高位，对于小的数则记录小的数本身
        res[j] = (byte) i;

        return res;
    }

    public int readVInt(byte[] res){
        int idx = 0;
        byte b = res[idx++];

        if( b >= 0) return b;

        int i = b & 0x7F;

        b = res[idx++];

        i |= (b & 0x7F) << 7;

        if(b >= 0) return i;

        b = res[idx++];

        i |= (b & 0x7F) << 14;

        if( b >= 0) return i;

        b = res[idx++];

        i |= (b & 0x7F) << 21;

        if( b >=0 ) return i;

        b = res[idx];

        i |= (b & 0x7F) << 28;

        if(( b & 0xF0) == 0)
            return i;

        throw new RuntimeException("Invalid int");
    }

    private int bytesRequired(int i) {
        if(i < 0) throw new RuntimeException("not suit");
        if((i >>> 7) == 0) return 1;
        if((i >>> 14) == 0) return 2;
        if((i >>> 21) == 0) return 3;
        if((i >>> 28) == 0) return 4;
        return 5;
    }

//    final void writeVInt(int i) {
//        byte[] res = new byte[20];
//        //0x7f 0111 1111    ~ ->  1000 0000
//        //0x80 1000 0000
//        int j = 0;
//        while ((i & ~0x7F) != 0) {
//            res[j++] = (byte) ((i & 0x7f) | 0x80);
//            i >>>= 7;
//        }
//
//        for (int k = 0; k < res.length; k++) {
//            System.out.print(res[k]+"    ");
//        }
//
//        System.out.println();
//
//        System.out.println("==============================");
//    }

}
