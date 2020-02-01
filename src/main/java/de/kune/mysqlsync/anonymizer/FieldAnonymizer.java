package de.kune.mysqlsync.anonymizer;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@FunctionalInterface
public interface FieldAnonymizer {

    List<String> FIRST_NAMES = unmodifiableList(new ArrayList<>(new LinkedHashSet<>(Arrays.asList("Robbie", "Gisele", "Emmaline", "Tess", "Jestine", "Ligia", "Veronika", "Chris", "Nohemi", "Latina", "Inge", "Steffanie", "Remedios", "Yajaira", "Miguelina", "Grover", "Shelli", "Maribel", "Raguel", "Alvaro", "Kristeen", "Amparo", "Long", "Darell", "Bell", "Alica", "Lue", "Lynnette", "Alberta", "Alice", "Myrta", "Luanna", "Lakeesha", "Rafael", "Eleni", "Joellen", "Margert", "Dean", "Tammy", "Dan", "Caren", "Antonina", "Helena", "Marcellus", "Gussie", "Cecil", "Earlie", "Felicia", "Stacy", "Ines", "Lani", "Gaye", "Kerri", "Mardell", "Shanel", "Rocky", "Christena", "Ciera", "Margarete", "Normand", "Keira", "Rosalina", "Chuck", "Amada", "Lawerence", "Ava", "Tracey", "Luana", "Kacie", "Elouise", "Ileana", "Lanie", "Althea", "Evelia", "Emerita", "Samuel", "Beulah", "Portia", "Chet", "Aiko", "Freddy", "Abigail", "Ella", "Jerri", "Erline", "Gary", "Faye", "Dorthea", "Joselyn", "Lemuel", "Zane", "Florence", "Winford", "Kaci", "Candis", "Noble", "Dorothy", "Tresa", "Alvina", "Gretta", "Pedro", "Princess", "Alyse", "Vernetta", "Deadra", "Rosamond", "Vanesa", "Rossana", "Alyce", "Han", "Shauna", "Buddy", "Herma", "Keren", "Clemente", "Alda", "Le", "Jordon", "Tana", "Liana", "Jeane", "Moises", "Yanira", "Daniella", "Lanora", "Lynwood", "Vernon", "Ella", "Marketta", "Faviola", "Bart", "Rosella", "Ammie", "Hyo", "Mariko", "Celsa", "Jaimie", "Cathleen", "Jodee", "Gwen", "Charlena", "Elmo", "Derick", "Juliana", "Tyesha", "Raphael", "Dannette", "Bronwyn", "Rodrigo", "Adena", "Carolyn", "Dia", "Chae", "Olivia", "Silvia", "Valentin", "Kindra", "Stefanie", "Tressie", "Marsha", "Chanell", "Magdalena", "Benita", "Ione", "Sondra", "Herman", "Kevin", "Verena", "Odessa", "Alberto", "Tyler", "Del", "Fiona", "Jacque", "Dovie", "Jacki", "Shira", "Domingo", "Ty", "Jeffry", "Ester", "Bianca", "Kimberlee", "Diann", "Rolande", "Manie", "Lovetta", "Charlena", "Shyla", "Eleonore", "Imelda", "Debbra", "Lyndia", "Maryrose", "Christine", "Missy", "Sana", "Shizue", "Patricia", "Sharonda"))));
    List<String> LAST_NAMES = unmodifiableList(new ArrayList<>(new LinkedHashSet<>(Arrays.asList("Mcclure", "Jarvis", "Kane", "Burke", "Olsen", "Stuart", "Terry", "Meza", "Medina", "Cooper", "Wise", "Mullins", "Gregory", "Lozano", "Scott", "Parsons", "Goodwin", "Leonard", "Cummings", "Morgan", "Melton", "Nguyen", "Galloway", "Nicholson", "Aguilar", "Tapia", "Norris", "Ford", "Mcconnell", "Lopez", "Walls", "Oneal", "Daniel", "Meyers", "Valenzuela", "Rangel", "Kelley", "Walton", "Carlson", "Johns", "Watts", "Webb", "Dodson", "Spears", "Herman", "Solomon", "Simon", "Briggs", "Maxwell", "Donovan", "Rodgers", "Bush", "Griffin", "Fry", "Lee", "Copeland", "Fitzgerald", "Valentine", "Carpenter", "Foster", "Ashley", "Randall", "Ibarra", "Clark", "Humphrey", "Hendrix", "Cordova", "Wilson", "Snyder", "Roman", "Brown", "Hooper", "Weber", "Coffey", "Stanley", "Vasquez", "Wu", "Howe", "Page", "Gordon", "Erickson", "Thompson", "Walsh", "Barrera", "Pollard", "Sandoval", "Tucker", "Benson", "Lara", "Barry", "Delacruz", "Hays", "Mathews", "Bernard", "Norton", "Landry", "Hurley", "Howell", "Carson", "Casey", "Saunders", "Farley", "Woodward", "Beltran", "Gill", "Levy", "Stafford", "Lamb", "Benitez", "Caldwell", "Kelly", "Rosario", "Petty", "Dyer", "Gray", "Leblanc", "Bautista", "Olson", "Carrillo", "Odom", "Randolph", "Sharp", "Esparza", "Harris", "Romero", "Hester", "Winters", "Shaw", "Michael", "Ingram", "Gentry", "Wong", "Pearson", "Conway", "Reyes", "Bender", "Doyle", "Hodges", "Vaughn", "Lynn", "Willis", "Shepard", "Hendricks", "Calderon", "Wiggins", "Mcdaniel", "Shepherd", "Vincent", "Salinas", "Joseph"))));
    List<String> CITIES = unmodifiableList(new ArrayList<>(new LinkedHashSet<>(Arrays.asList("Landfall", "Braxton", "Fanwood", "Celebration", "Mounds View", "Cedar Creek", "Rhome", "Veteran", "Nowthen", "Halchita", "Micro", "Lund", "Oak Grove Heights", "Soda Bay", "Wendell", "Hydesville", "Prentice", "Lake Wissota", "Dalhart", "East Hodge", "Jacksonville Beach", "Maytown", "Ammon", "Gainesville", "Fort Mohave", "Crown Point", "Stantonville", "Port Orchard", "Tanacross", "Bell Arthur", "Daviston", "Fort Belvoir", "Neibert", "Pilgrim", "Grant", "Vann Crossroads", "City of the Sun", "South Coffeyville", "South Milwaukee", "Stuttgart", "Spring Mount", "Deatsville", "East Fork", "Waialua", "Burnt Ranch", "Hitchita", "Ruch", "Audubon", "Oxnard", "East Avon", "Kibler", "Paoli", "Elysian", "Oriental", "Zellwood", "Long Prairie", "White Mountain", "Copake Lake", "Arden on the Severn", "White Meadow Lake", "Narciso Pena", "Persia", "Ponderosa Pines", "Carlyss", "Sunrise Beach Village", "Kouts", "Williamsburg", "Max Meadows", "Whispering Pines", "Stratmoor", "Newfield Hamlet", "Knobel", "Mallard", "Gore", "Hayfield", "Roxboro", "Leawood", "Clear Lake Shores", "Linglestown", "Minor", "Finn Hill", "Aulander", "Bonadelle Ranchos", "Arcadia", "Pope-Vannoy Landing", "Wayne Heights", "Byersville", "Lockhart", "Larwill", "Magnet", "Staunton", "Garden Farms", "Lynnwood", "Stoneham", "Condon", "Sands Point", "Cokeville", "McNeal", "Wilkesboro", "Boothville"))));
    List<String> STREETS = unmodifiableList(new ArrayList<>(new LinkedHashSet<>(Arrays.asList("Fawn Court", "Main Street South", "Old York Road", "Rosewood Drive", "2nd Street North", "Orange Street", "6th Avenue", "Bridle Lane", "Shady Lane", "Warren Avenue", "Cross Street", "Woodland Avenue", "Myrtle Avenue", "Cambridge Court", "6th Street", "Dogwood Lane", "Route 64", "Hamilton Street", "4th Street West", "Lexington Drive", "Arch Street", "Heather Lane", "Meadow Lane", "4th Avenue", "Laurel Drive", "Sheffield Drive", "Route 30", "Front Street North", "Oak Lane", "Windsor Court", "Route 10", "Grand Avenue", "5th Street", "Jones Street", "Lincoln Street", "Hillside Drive", "James Street", "Center Street", "Ivy Lane", "13th Street", "Route 20", "Beech Street", "Virginia Avenue", "Manor Drive", "Pennsylvania Avenue", "Cleveland Avenue", "Cedar Court", "Pine Street", "River Road", "Devon Road", "Woodland Drive", "Sunset Drive", "Cherry Lane", "Canterbury Drive", "Franklin Avenue", "Country Lane", "Ann Street", "Lilac Lane", "Liberty Street", "Cobblestone Court", "8th Avenue", "Orchard Street", "Devonshire Drive", "Meadow Street", "Willow Street", "Bridle Court", "Cambridge Road", "Lexington Court", "Creek Road", "Pheasant Run", "Canal Street", "Lakeview Drive", "5th Street West", "West Avenue", "Summit Street", "Chestnut Street", "Jefferson Avenue", "Andover Court", "College Avenue", "Orchard Lane", "4th Street North", "Spruce Street", "Grove Street", "School Street", "Cleveland Street", "Lake Street", "Forest Drive", "Canterbury Road", "Evergreen Lane", "Walnut Avenue", "Railroad Avenue", "Circle Drive", "Route 4", "Sycamore Drive", "5th Street East", "Parker Street", "Morris Street", "Cedar Street", "Devon Court", "Prospect Street", "Pearl Street", "Summer Street", "Canterbury Court", "Hawthorne Lane", "Hickory Street", "Ridge Road", "Hanover Court", "Colonial Drive", "Linda Lane", "Briarwood Drive", "Sycamore Street", "Washington Street", "Glenwood Avenue", "Aspen Court", "Mulberry Lane", "Augusta Drive", "Arlington Avenue", "Front Street South", "Franklin Court", "Belmont Avenue", "Monroe Drive", "Harrison Street", "Prospect Avenue", "Madison Court", "South Street", "John Street", "Country Club Drive", "Hillcrest Drive", "Valley View Road", "Hamilton Road", "Tanglewood Drive", "Buttonwood Drive", "Adams Avenue", "Riverside Drive", "Homestead Drive", "Eagle Road", "Route 7", "West Street", "Lantern Lane", "Elizabeth Street", "Bank Street", "Forest Avenue", "Cardinal Drive", "Route 70", "Main Street East", "8th Street", "Chestnut Avenue", "Madison Avenue", "Smith Street", "Brookside Drive"))));
    List<String> STREET_NUMBERS = IntStream.range(1, 180).boxed().map(i->Integer.toString(i)).collect(toList());


    FieldAnonymizer DEFAULT = (k, v) -> hash(v);
    FieldAnonymizer DEFAULT_RETAIN_LENGTH = (k, v) -> trimToSameLength(hash(v), v);

    static String trimToSameLength(String hash, Object k) {
        if (hash == null) {
            return null;
        }
        if (k == null) {
            return "";
        }
        if (hash.length() > k.toString().length()) {
            return hash.substring(0, k.toString().length());
        }
        if (hash.length() < k.toString().length()) {
            return hash + hash.substring(0, (k.toString().length() - hash.length()));
        }
        return hash;
    }

    FieldAnonymizer CITY = (k, v) -> hash(CITIES, v);
    FieldAnonymizer FIRST_NAME = (k, v) -> hash(FIRST_NAMES, v);
    FieldAnonymizer LAST_NAME = (k, v) -> hash(LAST_NAMES, v);
    FieldAnonymizer LAST_NAME_FIRST_NAME = (k, v) -> LAST_NAME.anonymize(k, v) + ", " + FIRST_NAME.anonymize(k, v);
    FieldAnonymizer STREET = (k, v) -> hash(STREETS, v);
    FieldAnonymizer STREET_NUMBER = (k, v) -> hash(STREET_NUMBERS, v);
    FieldAnonymizer PHONE = (k, v) -> "+" + hash(v);
    FieldAnonymizer POST_CODE = (k, v) -> v == null ? null : Long.toString(Math.abs(hashNumber(v)) % 100000);
    FieldAnonymizer IBAN = (k, v) -> {
        if (v == null) {
            return null;
        }
        if (v.equals("")) {
            return "";
        }
        if (v.toString().length() < 2) {
            return v.toString();
        }
        String prefix = v.toString().substring(0, 2);
        if (prefix.toLowerCase().equals("de")) {
            long number = Math.abs(hashNumber(v)) % 1000000000000000000L;
            String checksum = format("%02d", BigInteger.valueOf(98).subtract(new BigInteger(Long.toString(number) + "131400").mod(BigInteger.valueOf(97))).intValue());
            return format("%s%s%018d", prefix, checksum, number);
        } else {
            return prefix + Math.abs(hashNumber(v));
        }
    };

    Map<Pattern, FieldAnonymizer> DEFAULT_ANONYMIZERS = new HashMap<Pattern, FieldAnonymizer>() {{
        put(Pattern.compile("^.*?\\.account_?[hH]older$"), LAST_NAME_FIRST_NAME);
        put(Pattern.compile("^.*?\\.street$"), STREET);
        put(Pattern.compile("^.*?\\.street_?[nN]umber$"), STREET_NUMBER);
        put(Pattern.compile("^.*?\\.post_?[cC]ode$"), POST_CODE);
        put(Pattern.compile("^.*?\\.city$"), CITY);
        put(Pattern.compile("^.*?\\.first_?[nN]ame$"), FIRST_NAME);
        put(Pattern.compile("^.*?\\.last_?[nN]ame$"), LAST_NAME);
        put(Pattern.compile("^.*?\\.birth_?[nN]ame$"), LAST_NAME);
        put(Pattern.compile("^.*?\\.phone_?[nN]umber$"), PHONE);
        put(Pattern.compile("^.*?\\.id_?[cC]ard_?[nN]umber$"), DEFAULT);
        put(Pattern.compile("^.*?\\.email_?[aA]ddress$"), DEFAULT);
        put(Pattern.compile("^.*?\\.tax_?[iI]d$"), DEFAULT_RETAIN_LENGTH);
        put(Pattern.compile("^.*?\\.tax_?[iI]dentification_?[nN]umber$"), DEFAULT);
        put(Pattern.compile("^.*?\\.iban$"), IBAN);
    }};

    String anonymize(String key, Object value);

    static String hash(Object input) {
        if (input == null) {
            return null;
        }
        if (input.equals("")) {
            return "";
        }
        return Long.toString(hashNumber(input));
    }

    static long hashNumber(Object input) {
        try {
            return Math.abs(new BigInteger(MessageDigest.getInstance("md5").digest(Stream.of(input).map(Object::toString).collect(joining()).getBytes(Charset.defaultCharset()))).longValue());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    static String hash(List<String> candidates, Object input) {
        if (input == null) {
            return null;
        }
        if (input.equals("")) {
            return "";
        }
        return Stream.of(input).map(s->candidates.get((int) (Math.abs(hashNumber(s) % candidates.size())))).collect(joining(" "));
    }

    static boolean isLike(String key, String pattern) {
        if (key == null) {
            return false;
        }
        return pattern.toLowerCase().equals(key.replace("_", "").replace("-", "").toLowerCase());
    }

}
