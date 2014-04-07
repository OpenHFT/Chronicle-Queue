/*
 * Copyright 2014 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.slf4j.tools;

import net.openhft.chronicle.IndexedChronicle;
import net.openhft.chronicle.VanillaChronicle;
import net.openhft.chronicle.slf4j.impl.AbstractBinaryChronicleLogReader;
import net.openhft.chronicle.slf4j.impl.AbstractTextChronicleLogReader;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public class ChroniGrep extends ChroniTool {

    // *************************************************************************
    //
    // *************************************************************************

    public static void main(String[] args) {
        try {
            boolean indexed = false;
            boolean binary = true;
            Grep grep = new Grep();

            //TODO add more options
            for (int i = 0; i < args.length - 1; i++) {
                if ("-t".equals(args[i])) {
                    binary = false;
                } else if ("-i".equals(args[i])) {
                    indexed = true;
                } else if (i != args.length - 1) {
                    grep.add(args[i]);
                }
            }

            if (args.length >= 1 && !grep.isEmpty()) {
                ChroniTool.process(
                        indexed
                                ? new IndexedChronicle(args[args.length - 1])
                                : new VanillaChronicle(args[args.length - 1]),
                        binary
                                ? new BinaryGrep(grep)
                                : new TextGrep(grep),
                        false,
                        false
                );
            } else {
                System.err.format("\nUsage: ChroniGrep [-t|-i] regexp1 ... regexpN path");
                System.err.format("\n  -t = text chronicle, default binary");
                System.err.format("\n  -i = IndexedCronicle, default VanillaChronicle");
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    // *************************************************************************
    //
    // *************************************************************************

    private static class Grep {
        private Set<String> regexps;

        public Grep() {
            this.regexps = new HashSet<String>();
        }

        public void add(String regexp) {
            this.regexps.add(regexp);
        }

        public boolean isEmpty() {
            return this.regexps.isEmpty();
        }

        boolean matches(String message) {
            for (String regexp : this.regexps) {
                if (message.matches(regexp)) {
                    return true;
                }
            }

            return false;
        }
    }

    ;

    private static final class BinaryGrep extends AbstractBinaryChronicleLogReader {
        private final Grep grep;

        public BinaryGrep(final Grep grep) {
            this.grep = grep;
        }

        @Override
        public void process(Date timestamp, int level, long threadId, String threadName, String name, String message, Object... args) {
            String msg = asString(timestamp, level, threadId, threadName, name, message, args);
            if (this.grep.matches(msg)) {
                System.out.println(msg);
            }
        }
    }

    ;

    // *************************************************************************
    //
    // *************************************************************************

    private static final class TextGrep extends AbstractTextChronicleLogReader {
        private final Grep grep;

        public TextGrep(final Grep grep) {
            this.grep = grep;
        }

        @Override
        public void process(String msg) {
            if (this.grep.matches(msg)) {
                System.out.println(msg);
            }
        }
    }
}
