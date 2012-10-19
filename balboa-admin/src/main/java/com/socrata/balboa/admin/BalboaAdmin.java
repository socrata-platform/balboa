package com.socrata.balboa.admin;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.socrata.balboa.admin.tools.Dumper;
import com.socrata.balboa.admin.tools.Checker;
import com.socrata.balboa.admin.tools.Filler;

import java.io.*;
import java.util.Arrays;

public class BalboaAdmin
{
    public static void usage()
    {
        System.err.println("Balboa admin utility:\n" +
           "\tjava -jar balboa-admin <command> [args]\n\n" +
           "Commands:\n" +
           "\tfsck [filters...] : Check the balboa file system and validate the correctness of the tiers. This will probably take a long time.\n" +
           "\tfill file         : Restore balboa metrics from [file].\n" +
           "\tdump [filters...] : Dump all of the data in a balboa store to stdout in a format suitable for fill, with an optional entity regex");
    }

    public static char pickEscapeCharacter(File f) throws IOException {
        boolean[] mask = new boolean[65536];

        InputStreamReader input = new InputStreamReader(new BufferedInputStream(new FileInputStream(f)));
        try {
            int c = -1;
            while ((c = input.read()) != -1) {
                if (c >= 0) {
                    mask[c] = true;
                }
            }
        } finally {
            input.close();
        }

        for (int i = 0; i < mask.length; i++) {
            if (!mask[i]) return (char) i;
        }
        throw new IOException("Unable to find a char that doesn't occur.  You're screwed.");
    }

    public static void main(String[] args) throws IOException
    {
        if (args.length == 0)
        {
            usage();
            System.exit(1);
        }

        String command = args[0];

        if (command.equals("fill"))
        {
            if (args.length < 2) {
                System.err.println("No file for fill specified '" + command + "'.");
                usage();
                System.exit(1);
            }
            String file = args[1];
            System.out.println("Reading from file " + file);
            System.out.println("Picking an escape character from the file which does not exist, because CSVReader can choke on it's own output if the default '\\' character exists.");
            System.out.println("\thttp://sourceforge.net/tracker/?func=detail&aid=2908769&group_id=148905&atid=773541");
            File inputFile = new File(file);
            char escape = pickEscapeCharacter(inputFile);
            System.out.println("Using escape character " + escape);
            CSVReader reader = new CSVReader(new InputStreamReader(new BufferedInputStream(new FileInputStream(inputFile))), ',', '"', escape);
            try
            {
                Filler filler = new Filler(reader);
                filler.fill();
            }
            finally
            {
                reader.close();
            }
        }
        else if (command.equals("dump"))
        {
            CSVWriter writer = new CSVWriter(new PrintWriter(System.out), ',', '"');
            try
            {
                Dumper dumper = new Dumper(writer);
                dumper.dump(Arrays.asList(args).subList(1, args.length));
            }
            finally
            {
                writer.close();
            }
        }
        else if (command.equals("fsck"))
        {
            Checker fsck = new Checker();
            fsck.check(Arrays.asList(args).subList(1, args.length));
        }
        else
        {
            System.err.println("Unknown command '" + command + "'.");
            usage();
            System.exit(1);
        }
    }
}
