package rdfjena;


import java.io.InputStream;

import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.SimpleSelector;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.util.FileManager;

public class RDFReader {

	public static void main(String[] args) {

		String inputFileName = "Berlin.rdf";
		Model model = ModelFactory.createDefaultModel();
	       
        // use the FileManager to find the input file
        InputStream in = FileManager.get().open(inputFileName);
        if (in == null) {
            throw new IllegalArgumentException( "File: " + inputFileName + " not found");
        }
        
        // read the RDF/XML file
        model.read( in, "");
        Property geoProperty = 
                model.createProperty("http://dbpedia.org/property/",
                        "origin");

                StmtIterator iter =
                    model.listStatements(new SimpleSelector(null, geoProperty,(RDFNode) null)); 

                //Loop to traverse the statements that match the SimpleSelector
                while (iter.hasNext()) {
                		Statement stmt = iter.nextStatement();
                	   System.out.print(stmt.getSubject().toString());
                	   System.out.print("   "+stmt.getPredicate().toString());
                	   System.out.println("  "+ stmt.getObject().toString());
                }   
            }   

}