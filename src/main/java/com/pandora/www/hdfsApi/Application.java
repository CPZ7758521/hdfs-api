package com.pandora.www.hdfsApi;

import com.pandora.www.hdfsApi.fileUtils.MyFileUtil;

import java.io.IOException;

public class Application {
    public static void main(String[] args) throws IOException {
//        整个文件夹下的文件都copy
        MyFileUtil.copyFiles();

//        删除指定目录下的表对应的文件
        MyFileUtil.deleteFile("");
    }
}
