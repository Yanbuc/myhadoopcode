package myoutput;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MyDBKey implements DBWritable, Writable {
    /*
    @Override
    public int compareTo(MyDBKey o) {
        if(this.id>o.id){
            return 1;
        }
        return -1;
    }
*/
    private int id;
    private String filename;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(filename);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       id=in.readInt();
       filename=in.readUTF();
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
         statement.setInt(1,id);
         statement.setString(2,filename);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
         id=resultSet.getInt("id");
         filename=resultSet.getString("filename");
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }
}
