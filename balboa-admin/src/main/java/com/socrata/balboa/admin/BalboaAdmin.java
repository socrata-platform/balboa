package com.socrata.balboa.admin;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import com.socrata.balboa.admin.tools.Dumper;
import com.socrata.balboa.admin.tools.Filler;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class BalboaAdmin
{
    public static void usage()
    {
        System.err.println("Balboa admin utility:\n" +
           "\tjava -jar balboa-admin <command> [args]\n\n" +
           "Commands:\n" +
           "\tfill: Restore balboa metrics from stdin.\n" +
           "\tdump: Dump all of the data in a balboa store to stdout in a format suitable for fill.");
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
            CSVReader reader = new CSVReader(new InputStreamReader(System.in), ',', '"');
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
                dumper.dump();
            }
            finally
            {
                writer.close();
            }
        }
        else
        {
            System.err.println("Unknown command '" + command + "'.");
            usage();
            System.exit(1);
        }
    }
}
