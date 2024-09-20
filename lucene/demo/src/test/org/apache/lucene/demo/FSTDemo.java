package org.apache.lucene.demo;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.*;

import java.io.IOException;
import java.util.Comparator;

public class FSTDemo {
    public static void main(String[] args) throws IOException {
        String[] inputValues = {"bat", "cat", "deep", "do", "dog", "dogs"};
        long[] outputvalues = {2, 5, 15, 10, 3, 2};

        PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
        FSTCompiler.Builder<Long> builder = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs);
        FSTCompiler<Long> build = builder.build();

        IntsRefBuilder intsRefBuilder = new IntsRefBuilder();
        for (int i = 0; i < inputValues.length; i ++) {
            //
            BytesRef bytesRef = new BytesRef(inputValues[i]);
            build.add(Util.toIntsRef(bytesRef, intsRefBuilder), outputvalues[i]);
        }

        FST<Long> fst = build.compile();

        BytesRef bytesRef = new BytesRef(inputValues[3]);

        Long aLong = Util.get(fst, Util.toIntsRef(bytesRef, intsRefBuilder));

        System.out.println(aLong);

        IntsRefFSTEnum<Long> fstEnum = new IntsRefFSTEnum<>(fst);
        IntsRefFSTEnum.InputOutput<Long> inputOutput;
        BytesRefBuilder scratch = new BytesRefBuilder();
        while((inputOutput = fstEnum.next())!= null){
            String input = Util.toBytesRef(inputOutput.input, scratch).utf8ToString();
            Long output = inputOutput.output;
            System.out.println(input + " \t" + output);
        }

        // 跟输入d自动补全
        String userInput = "d";
        BytesRef bytesRefUserInput = new BytesRef(userInput);
        IntsRef intsRefUserInput = Util.toIntsRef(bytesRefUserInput, new IntsRefBuilder());
        FST.Arc<Long> arc = fst.getFirstArc(new FST.Arc<>());
        for (int i = 0; i < intsRefUserInput.length; i ++) {
            if (fst.findTargetArc(intsRefUserInput.ints[intsRefUserInput.offset + i], arc, arc, fst.getBytesReader()) == null) {
                System.out.println("没找到d开头的");
            }
        }

        Util.TopResults<Long> results = Util.shortestPaths(fst, arc, PositiveIntOutputs.getSingleton().getNoOutput(), new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return o1.compareTo(o2);
            }
        }, 3, false);

        BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();
        for (Util.Result<Long> result : results) {
            IntsRef intsRef = result.input;
            System.out.println(userInput + Util.toBytesRef(intsRef, bytesRefBuilder).utf8ToString());
        }


    }
}
