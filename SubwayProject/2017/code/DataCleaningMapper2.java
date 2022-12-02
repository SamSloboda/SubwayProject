import java.io.IOException;
import java.util.ArrayList;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataCleaningMapper2 extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        // Iterate through each line
        // Set line as array delimited
        String[] line = value.toString().split(",");
        boolean violentCrime = false;
        String date = "";
        String time = "";
        String xcoord = "";
        String ycoord = "";
        String crimeDescription = "";
        // Check if there is an error in the csv

        if(line[1].length() == 10 ){     //checking for missing date ==> DROPPING IF MISSING
            String [] dateList = line[1].split("-");
            date = dateList[1] + dateList[2] + dateList[0];
        }else{
            return;
        }

        if(!(line[2].equals("(null)"))){    //checking for missing time ==> DROPPING IF MISSING
            SimpleDateFormat displayFormat = new SimpleDateFormat("HH:mm");
            SimpleDateFormat parseFormat = new SimpleDateFormat("hh:mm:ss a");
            try{
                Date dateDate = parseFormat.parse(line[2]);
                date = displayFormat.format(dateDate).toString().replace(":","");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }else{
            return;
        }

        if((line[78].length() == 9) && (line[79].length() == 9)){ //checking if the record has geolocation. If not, dropping imidiatelly.
            xcoord = line[78].trim().substring(0,6);
            ycoord = line[79].trim().substring(0,6);
        }else{
            return;
        }

        //checking if any weapon was used. If yes, it will be automatically considered violent crime.
        if(line[33].equalsIgnoreCase("Y") || line[34].equalsIgnoreCase("Y") || line[35].equalsIgnoreCase("Y") || line[36].equalsIgnoreCase("Y")){
            violentCrime = true;
            crimeDescription = "WEAPON_FOUND";
        }

        //checking if physical force was used. If yes, it will be automatically considered violent crime.
        if(line[37].equalsIgnoreCase("Y") || line[38].equalsIgnoreCase("Y") || line[39].equalsIgnoreCase("Y") || line[40].equalsIgnoreCase("Y") || line[41].equalsIgnoreCase("Y") || line[42].equalsIgnoreCase("Y") || line[43].equalsIgnoreCase("Y") || line[44].equalsIgnoreCase("Y")){
            violentCrime = true;
            crimeDescription = "PHYSICAL_FORCE_USED";

        }

        /*
        Creating a ArrayList of all the crimes we considered as VIOLENT
        */
        ArrayList<String> violentCrimesDescriptions = new ArrayList<String>();
        
        violentCrimesDescriptions.add("AGGRAVATED ASSAULT");
        violentCrimesDescriptions.add("AGGRAVATED HARASSMENT");
        violentCrimesDescriptions.add("AGGRAVATED SEXUAL ABUSE");
        violentCrimesDescriptions.add("ARSON");
        violentCrimesDescriptions.add("ASSAULT");
        violentCrimesDescriptions.add("COERCION");
        violentCrimesDescriptions.add("COURSE OF SEXUAL CONDUCT");
        violentCrimesDescriptions.add("CPW");
        violentCrimesDescriptions.add("CRIMINAL POSSESION OF CONTROLLED SUBSTANCE");
        violentCrimesDescriptions.add("CRIMINAL POSSESSION OF FORGED INSTRUMENT");
        violentCrimesDescriptions.add("CRIMINAL SALE OF CONTROLLED SUBSTANCE");
        violentCrimesDescriptions.add("ENDANGER THE WELFARE OF A CHILD");
        violentCrimesDescriptions.add("ESCAPE");
        violentCrimesDescriptions.add("FRAUDULENT ACCOSTING");
        violentCrimesDescriptions.add("HARASSMENT");
        violentCrimesDescriptions.add("HAZING");
        violentCrimesDescriptions.add("HINDERING PROSECUTION");
        violentCrimesDescriptions.add("INCEST");
        violentCrimesDescriptions.add("KIDNAPPING");
        violentCrimesDescriptions.add("KILLING OR INJURING A POILCE ANIMAL"); //could be problem here...mispelled POLICE
        violentCrimesDescriptions.add("MENACING");
        violentCrimesDescriptions.add("MURDER");
        violentCrimesDescriptions.add("OBSCENITY");
        violentCrimesDescriptions.add("OBSTRUCTING FIREFIGHTING OPERATIONS");
        violentCrimesDescriptions.add("PROHIBITED USE OF WEAPON");
        violentCrimesDescriptions.add("PROMOTING SUICIDE");
        violentCrimesDescriptions.add("RAPE");
        violentCrimesDescriptions.add("RECKLESS ENDANGERMENT");
        violentCrimesDescriptions.add("RESISTING ARREST");
        violentCrimesDescriptions.add("RIOT");
        violentCrimesDescriptions.add("SEXUAL ABUSE");
        violentCrimesDescriptions.add("SEXUAL MISCONDUCT");
        violentCrimesDescriptions.add("SEXUAL PERFORMANCE BY A CHILD");
        violentCrimesDescriptions.add("SODOMY");
        violentCrimesDescriptions.add("TERRORISM");
        violentCrimesDescriptions.add("UNLAWFULLY DEALING WITH FIREWORKS");
        violentCrimesDescriptions.add("UNLAWFULL IMPRISONMENT");
        violentCrimesDescriptions.add("UNLAWFULLY DEALING WITH A CHILD");
        violentCrimesDescriptions.add("VEHICULAR ASSAULT");
        violentCrimesDescriptions.add("FORCIBLE TOUCHING");

        // checking if the detailCM matches with the Hash Map of considered violent crimes, if yes it is concidered as violent crime. If it's missing we are still keeping this record and marking it as UNKNOWN.
        if(violentCrimesDescriptions.contains(line[17])){
            crimeDescription = line[17];
            violentCrime = true;
        }else if(line[17].equals("(null)")){
            crimeDescription = "UNKNOWN";
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