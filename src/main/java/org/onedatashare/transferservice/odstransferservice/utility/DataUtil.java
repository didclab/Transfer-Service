package org.onedatashare.transferservice.odstransferservice.utility;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @author deepika
 */
public class DataUtil {

    public static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);

    public static String getStringDate(Date date){
        return format.format(date);
    }

    public static Date getDate(String date) throws ParseException {
        return format.parse(date);
    }

}
