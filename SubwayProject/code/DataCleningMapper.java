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
        boolean violentCrime = false;
        // Check if there is an error in the csv

        if(!line[3].eqauls("")){     //checking for missing date ==> DROPPING IF MISSING
            String date = line[3];
        }else{
            return;
        }

        if(!line[4].eqauls("")){    //checking for missing time ==> DROPPING IF MISSING
            String time = line[4];
        }else{
            return;
        }

        if(!line[107].eqauls("") && !line[108].eqauls("")){ //checking if the record has geolocation. If not, dropping imidiatelly.
            String xcoord = line[107];
            String ycoord = line[108];
        }else{
            return;
        }

        // checking if the detailCM matches with the Hash Map of considered violent crimes, if yes it is concidered as violent crime. If it's missing we are still keeping this record and marking it as UNKNOWN.
        if(crimeIDcrimeDescriptions.containsKey(line[111])){
            String crimeDescription = crimeIDcrimeDescriptions.get(line[111]);
            violentCrime = true;
        }else if(line[111].eqauls("")){
            String crimeDescription = "UNKNOWN";
        }

        //checking if any weapon was used. If yes, it will be automatically considered violent crime.
        if(line[26].equalsIgnoreCase("Y") || line[27].equalsIgnoreCase("Y") || line[28].equalsIgnoreCase("Y") || line[29].equalsIgnoreCase("Y") || line[30].equalsIgnoreCase("Y") || line[31].equalsIgnoreCase("Y")){
            violentCrime = true;
        }

        //checking if physical force was used. If yes, it will be automatically considered violent crime.
        if(line[32].equalsIgnoreCase("Y") || line[33].equalsIgnoreCase("Y") || line[34].equalsIgnoreCase("Y") || line[35].equalsIgnoreCase("Y") || line[36].equalsIgnoreCase("Y") || line[37].equalsIgnoreCase("Y") || line[38].equalsIgnoreCase("Y") || line[39].equalsIgnoreCase("Y") || line[40].equalsIgnoreCase("Y")){
            violentCrime = true;
        }

        //checking if REASON FOR FRISK was VIOLENT CRIME SUSPECTED. If yes, consider it as violent crime.
        if(line[44].equalsIgnoreCase("Y") || line[44].eqauls("")){
            violentCrime = true;
        }



        if (violentCrime){
            // build output
            StringBuilder mapOutput = new StringBuilder();
            mapOutput.append(date + ","); // Date
            mapOutput.append(time + ","); // Time
            mapOutput.append(crimeDescription + ","); // Description of Crime
            mapOutput.append(xcoord + ","); // Xcoords
            mapOutput.append(ycoord); // Ycoords

            context.write(NullWritable.get(), new Text(mapOutput.toString()));
        }
    }

}