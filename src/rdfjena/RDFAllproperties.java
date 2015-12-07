package rdfjena;


import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.util.FileManager;

public class RDFAllproperties {
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {

		String inputFileName = "Berlin.rdf";
		Model model = ModelFactory.createDefaultModel();
	       
        // use the FileManager to find the input file
        InputStream in = FileManager.get().open(inputFileName);
        if (in == null) {
            throw new IllegalArgumentException( "File: " + inputFileName + " not found");
        }
        model.read( in, "");
        StmtIterator iter;
        Statement stmt;
        iter = model.listStatements();
        PrintWriter writer = new PrintWriter("output.txt", "UTF-8");
	    while (iter.hasNext()) {
	    	 stmt = iter.next();
	    	 String newLine="";
	    	System.out.println();
	        newLine=newLine+""+stmt.getSubject().getLocalName()+",";
	        newLine=newLine+""+stmt.getPredicate().getLocalName()+",";
	        if(stmt.getObject().isURIResource())
	        newLine=newLine+""+stmt.getObject().asResource().getLocalName();
	        writer.println(newLine);

	    }	
	    writer.close();
}
}
