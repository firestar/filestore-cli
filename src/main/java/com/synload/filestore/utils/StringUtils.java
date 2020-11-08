package com.synload.filestore.utils;

public class StringUtils {
    public static String hashToPath(String hash){
        int slashes = 4;
        char[] output = new char[hash.length()+slashes];
        int count = 0;
        int p=0;
        for(char c: hash.toCharArray()){
            if(count==2 && slashes>0){
                output[p]='/';
                p++;
                slashes--;
                count=0;
            }
            output[p] = c;
            count++;
            p++;
        };
        return new String(output);
    }
}
