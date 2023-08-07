package com.example;

import java.util.ArrayList;
import java.util.List;

class MessageParsedTuple {
    List<String> parts;
    String finalString;

    public MessageParsedTuple(List<String> parts, String finalString) {
        this.parts = parts;
        this.finalString = finalString;
    }
}

public class CommonUtils {

    public MessageParsedTuple split(String str, String delim) {
        List<String> parts = new ArrayList<String>();

        while(true) {
            int pos = str.indexOf(delim);
            if (pos >= 0) {
                String sub = str.substring(0, pos);
                if (sub.length() > 0) {
                    parts.add(sub);
                }
                str = str.substring(pos+delim.length());
            }
            else {
                break;
            }
        }

        return new MessageParsedTuple(parts, str);
    }
}
