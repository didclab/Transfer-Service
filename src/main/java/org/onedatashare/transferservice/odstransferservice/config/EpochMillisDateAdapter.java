package org.onedatashare.transferservice.odstransferservice.config;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.Date;

public class EpochMillisDateAdapter extends TypeAdapter<Date> {

    @Override
    public Date read(JsonReader in) throws IOException {
        return new Date(in.nextLong());
    }

    @Override
    public void write(JsonWriter out, Date value) throws IOException {
        out.value(value.getTime());
    }
}
