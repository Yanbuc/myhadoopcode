package dbinputformat;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Watchable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DbFile implements DBWritable {
    private  int id;
    private  String fileName;
/*
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(fileName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
         id=in.readInt();
         fileName=in.readUTF();
    }
*/
    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1,id);
        statement.setString(2,fileName);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
             id=resultSet.getInt("id");
             fileName=resultSet.getString("filename");
    }

    public int getId() {
        return id;
    }

    public String getFileName() {
        return fileName;
    }
}
