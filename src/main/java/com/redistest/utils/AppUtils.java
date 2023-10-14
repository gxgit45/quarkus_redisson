package com.redistest.utils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
public class AppUtils {

    public static String compressStringToBase64(String srcTxt)
            throws IOException {
        ByteArrayOutputStream rstBao = new ByteArrayOutputStream();
        GZIPOutputStream zos = new GZIPOutputStream(rstBao);
        zos.write(srcTxt.getBytes());
        IOUtils.closeQuietly(zos);
        byte[] bytes = rstBao.toByteArray();
        return Base64.encodeBase64String(bytes);
    }

    public static String compressStringToGzipStr(String srcTxt)
            throws IOException {
        ByteArrayOutputStream rstBao = new ByteArrayOutputStream();
        GZIPOutputStream zos = new GZIPOutputStream(rstBao);
        zos.write(srcTxt.getBytes());
        IOUtils.closeQuietly(zos);
        return rstBao.toString(StandardCharsets.UTF_8);
    }

    public static String uncompressString(String zippedBase64Str)
            throws IOException {
        String result = null;
        byte[] bytes = Base64.decodeBase64(zippedBase64Str);
        GZIPInputStream zi = null;
        try {
            zi = new GZIPInputStream(new ByteArrayInputStream(bytes));
            result = IOUtils.toString(zi);
        } finally {
            IOUtils.closeQuietly(zi);
        }
        return result;
    }

    public static String uncompressGzipString(String zippedBase64Str)
            throws IOException {
        String result = null;
        byte[] bytes = zippedBase64Str.getBytes(StandardCharsets.UTF_8);
        GZIPInputStream zi = null;
        try {
            zi = new GZIPInputStream(new ByteArrayInputStream(bytes));
            result = IOUtils.toString(zi);
        } finally {
            IOUtils.closeQuietly(zi);
        }
        return result;
    }
}
