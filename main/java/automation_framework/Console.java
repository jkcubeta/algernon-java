package automation_framework;

import java.text.ParseException;
import java.util.Scanner;
import java.util.function.IntBinaryOperator;

class Console {
	private static Scanner console;

	Console(){
		console = new Scanner(System.in);
	}

	String getPassword(){
		System.out.println("password...");
		return console.nextLine();
	}

	String getAddress(){
		System.out.println("database address");
		return console.nextLine();
	}

	Integer getScrubThreadCount(){
        Integer numberCount = null;
        Boolean checked = false;
        do {
            System.out.println("number of eligibility scrubbers");
            String count = console.nextLine();
            try {
                numberCount = Integer.parseInt(count);
                checked = true;
            } catch (NumberFormatException e) {
                System.out.println("that doesn't seem like a valid entry, try again");
            }
        }while(!checked);
        return numberCount;
	}

	Boolean getRedXGo(){
        int q =0;
        Boolean checked = null;
	    do{
		    System.out.println("run red x scrubber? (y/n)");
		    String count = console.nextLine();
		    if(count.equals("y")) {
                q++;
                checked = true;
            }
             else if(count.equals("n")) {
                q++;
                checked = false;
            }else if(!count.equals("y") && !count.equals("n")){
                System.out.println("huh? what? it's either y or n, it's not complicated, try again");
            }
		}while( q==0 );
        return checked;
	}

	Integer getRosterGo(){
        Integer numberCount = null;
        Boolean checked = false;
        do {
            System.out.println("number of table scrubbers");
            String count = console.nextLine();
            try {
                numberCount = Integer.parseInt(count);
                checked = true;
            } catch (NumberFormatException e) {
                System.out.println("that doesn't seem like a valid entry, try again");
            }
        }while(!checked);
        return numberCount;
	}
}
