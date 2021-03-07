package mapreduce_sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SortBean implements WritableComparable<SortBean> {

    private String word;
    private int num;

    /**
     * 先根据 word 字典序排序，再根据 num 排序
     */
    @Override
    public int compareTo(SortBean o) {
        //字符串自带的 compareTo
        int result = this.word.compareTo(o.word);
        if (result == 0){
            return this.num - o.num;
        }
        return result;
    }
    //序列化（转化为字节流）
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(num);
    }
    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = dataInput.readUTF();
        this.num = dataInput.readInt();
    }



    @Override
    public String toString() {
        return word + "\t" + num;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
