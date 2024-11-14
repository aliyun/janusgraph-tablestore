// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.tablestore;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

public class TableStoreColumnBuilder {
    public static byte[] encodeColumn(byte[] column) {
        if (column == null) {
            return null;
        }
        String encodedString = Hex.encodeHexString(column);
        try {
            return encodedString.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static byte[] decodeColumn(byte[] encodedColumn) {
        if (encodedColumn == null) {
            return null;
        }
        String encodedString = convertBytesToUTF8(encodedColumn,0,encodedColumn.length);
        try {
            return Hex.decodeHex(encodedString.toCharArray());
        } catch (DecoderException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static String convertBytesToUTF8(byte[] b, int off,int len) {
        if (b == null) {
            return null;
        }

        if (len == 0) {
            return "";
        }
        byte[] result = new byte[len];
        System.arraycopy(b, off, result, 0, len);

        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);

        try {
            CharBuffer charBuffer = decoder.decode(ByteBuffer.wrap(result));
            return charBuffer.toString();
        } catch (CharacterCodingException e) {
            // not valid utf-8
            throw new IllegalArgumentException("UTF-8 not supported: ", e);
        }
    }
}
