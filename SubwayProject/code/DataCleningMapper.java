import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataCleaningMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
     /*
    Creating a HashMap of all the crimes we considered as VIOLENT <KEY=detailCM, VALUE= Crime Description>
    */
    HashMap<String, String> crimeIDcrimeDescriptions = new HashMap<String, String>();
    crimeIDcrimeDescriptions.put("5","AGGRAVATED ASSAULT");
    crimeIDcrimeDescriptions.put("6","AGGRAVATED HARASSMENT");
    crimeIDcrimeDescriptions.put("7","AGGRAVATED SEXUAL ABUSE");
    crimeIDcrimeDescriptions.put("8","ARSON");
    crimeIDcrimeDescriptions.put("9","ASSAULT");
    crimeIDcrimeDescriptions.put("15","COERCION");
    crimeIDcrimeDescriptions.put("18","COURSE OF SEXUAL CONDUCT");
    crimeIDcrimeDescriptions.put("20","CPW");
    crimeIDcrimeDescriptions.put("24","CRIMINAL POSSESION OF CONTROLLED SUBSTANCE");
    crimeIDcrimeDescriptions.put("26","CRIMINAL POSSESSION OF FORGED INSTRUMENT");
    crimeIDcrimeDescriptions.put("28","CRIMINAL SALE OF CONTROLLED SUBSTANCE");
    crimeIDcrimeDescriptions.put("34","ENDANGER THE WELFARE OF A CHILD");
    crimeIDcrimeDescriptions.put("35","ESCAPE");
    crimeIDcrimeDescriptions.put("41","FRAUDULENT ACCOSTING");
    crimeIDcrimeDescriptions.put("47","HARASSMENT");
    crimeIDcrimeDescriptions.put("48","HAZING");
    crimeIDcrimeDescriptions.put("49","HINDERING PROSECUTION");
    crimeIDcrimeDescriptions.put("50","INCEST");
    crimeIDcrimeDescriptions.put("56","KIDNAPPING");
    crimeIDcrimeDescriptions.put("57","KILLING OR INJURING A POILCE ANIMAL"); //could be problem here...mispelled POLICE
    crimeIDcrimeDescriptions.put("60","MENACING");
    crimeIDcrimeDescriptions.put("62","MURDER");
    crimeIDcrimeDescriptions.put("63","OBSCENITY");
    crimeIDcrimeDescriptions.put("64","OBSTRUCTING FIREFIGHTING OPERATIONS");
    crimeIDcrimeDescriptions.put("72","PROHIBITED USE OF WEAPON");
    crimeIDcrimeDescriptions.put("73","PROMOTING SUICIDE");
    crimeIDcrimeDescriptions.put("77","RAPE");
    crimeIDcrimeDescriptions.put("78","RECKLESS ENDANGERMENT");
    crimeIDcrimeDescriptions.put("82","RESISTING ARREST");
    crimeIDcrimeDescriptions.put("84","RIOT");
    crimeIDcrimeDescriptions.put("87","SEXUAL ABUSE");
    crimeIDcrimeDescriptions.put("88","SEXUAL MISCONDUCT");
    crimeIDcrimeDescriptions.put("89","SEXUAL PERFORMANCE BY A CHILD");
    crimeIDcrimeDescriptions.put("90","SODOMY");
    crimeIDcrimeDescriptions.put("95","TERRORISM");
    crimeIDcrimeDescriptions.put("98","UNLAWFULLY DEALING WITH FIREWORKS");
    crimeIDcrimeDescriptions.put("108","UNLAWFULL IMPRISONMENT");
    crimeIDcrimeDescriptions.put("109","UNLAWFULLY DEALING WITH A CHILD");
    crimeIDcrimeDescriptions.put("111","VEHICULAR ASSAULT");
    crimeIDcrimeDescriptions.put("113","FORCIBLE TOUCHING");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Iterate through each line
        // Set line as array delimited
        String[] line = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        // Check if there is an error in the csv
        



        if (line.length < 16) {
            return;
        }
        // Filter out non-felonies
        if (!line[7].equalsIgnoreCase("f")) {
            return;
        }
        // build output
        StringBuilder mapOutput = new StringBuilder();
        mapOutput.append(line[0] + ","); // Arrest Key
        mapOutput.append(line[1] + ","); // Arrest Date
        mapOutput.append(line[2] + ","); // Arrest classification code
        mapOutput.append(line[3] + ","); // Arrest classification code description
        mapOutput.append(line[8] + ","); // Arrest bourough
        mapOutput.append(line[16] + ","); // Latitude
        mapOutput.append(line[17]); // Longitude

        context.write(NullWritable.get(), new Text(mapOutput.toString()));
    }

}