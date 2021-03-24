package net.streamproject.dataretrieval;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import be.ugent.rml.Executor;
import be.ugent.rml.Utils;
import be.ugent.rml.functions.FunctionLoader;
import be.ugent.rml.functions.lib.IDLabFunctions;
import be.ugent.rml.records.RecordsFactory;
import be.ugent.rml.store.QuadStore;
import be.ugent.rml.store.QuadStoreFactory;
import be.ugent.rml.store.RDF4JStore;

@RestController
public class MappingController {
	private final String endpoint = "http://localhost:8890/sparql/";
	private String getEndpoint() {
		Map<String, String> env = System.getenv();
		if (env.containsKey("SPARQL_URL"))
			return env.get("SPARQL_URL");
		return endpoint;
	}

	/**
	 * GET route for reading a mapping from the SPARQL endpoint
	 * @param id
	 * @param repository
	 * @return
	 */
	@GetMapping("/mapping")
	public Mapping mapping(@RequestParam(value = "id", defaultValue = "") String id, @RequestParam(value = "repository", defaultValue = "NOMAD") DataRepository repository) {
		return this.readMapping(id, repository);
	}
	
	// See https://jena.apache.org/documentation/query/app_api.html
	private Mapping readMapping(String id, DataRepository repository) {
		Mapping mapping = new Mapping(id, Status.UNKNOWN, repository, "", "");
		
		try {
			// check RDF graph if the id on the repository is known and what the status is
			String queryString = "PREFIX d: <http://dataretrieval.stream-projekt.net/>  SELECT * FROM NAMED <http://dataretrieval.stream-projekt.net/> { GRAPH ?g { ?s ?p ?o . ?s a d:Mapping . ?s d:id \""+id+"\" . ?s d:Repository d:"+repository+" } }" ;
			QueryExecution qexec = QueryExecutionFactory.sparqlService(this.getEndpoint(), queryString, this.getClient());
			ResultSet results = qexec.execSelect() ;
		    for ( ; results.hasNext() ; )
		    {
		      QuerySolution soln = results.nextSolution() ;
		      RDFNode x = soln.get("s") ;       // Get a result variable by name.
		      Resource r = soln.getResource("p") ; // Get a result variable - must be a resource
		      RDFNode l = soln.get("o") ;   // Get a result variable by name.
		      System.out.println(x.toString() + " " + r.toString() + " " + l.toString() + " .");
		      
		      switch (r.toString()) {
			      case "http://dataretrieval.stream-projekt.net/Status":
			    	  mapping.setStatus(Status.valueOf(l.toString()));
			    	  break;
			      case "http://dataretrieval.stream-projekt.net/Error":
			    	  mapping.setError(l.toString());
			    	  break;
			      case "http://dataretrieval.stream-projekt.net/TargetGraph":
			    	  mapping.setTargetGraph(l.toString());
			    	  break;
		      }
		    }
		} catch (Exception e) {
            e.printStackTrace();
			mapping.setStatus(Status.ERROR);
			mapping.setError(e.getMessage());
		}
		
		return mapping;
	}
	
	// TODO write mapping into graph on error before sending it
	@GetMapping("/startExecution")
	public Mapping startExecution(@RequestParam(value = "id", defaultValue = "") String id, @RequestParam(value = "repository", defaultValue = "NOMAD") DataRepository repository) {
		// check RDF graph if the id on the repository is known and what the status is
		Mapping mapping = this.readMapping(id, repository);
		if (mapping.getStatus() == Status.RUNNING)
			return mapping;
		
		//TODO <- if it is not running, then get the data and the mappings, start RMLMapper with it, write it into a specific graph and return the graph URI
		JSONObject nomad_archive_result = this.callNOMAD(id);
		if (nomad_archive_result.containsKey("error")) {
			mapping.setStatus(Status.ERROR);
			mapping.setError(nomad_archive_result.get("error").toString());
			return mapping;
		}
		
		String rml_mapping = this.getRMLMapping(mapping);
		if (rml_mapping.startsWith("ERROR:")) {
			mapping.setStatus(Status.ERROR);
			mapping.setError(rml_mapping);
			return mapping;
		}

		String mapping_result = "";
        try { // See https://github.com/RMLio/rmlmapper-java/blob/master/src/test/java/be/ugent/rml/readme/ReadmeTest.java
        	// Get the mapping string stream
            InputStream mappingStream = new ByteArrayInputStream(rml_mapping.getBytes());

            // Load the mapping in a QuadStore
            QuadStore rmlStore = QuadStoreFactory.read(mappingStream);

            // Set up the basepath for the records factory, i.e., the basepath for the (local file) data sources
            RecordsFactory factory = new RecordsFactory("/tmp");

            // Set up the functions used during the mapping
            Map<String, Class> libraryMap = new HashMap<>();
            libraryMap.put("IDLabFunctions", IDLabFunctions.class);

            FunctionLoader functionLoader = new FunctionLoader(null, libraryMap);

            // Set up the outputstore (needed when you want to output something else than nquads
            QuadStore outputStore = new RDF4JStore();

            // Create the Executor
            Executor executor = new Executor(rmlStore, factory, functionLoader, outputStore, Utils.getBaseDirectiveTurtle(mappingStream));

            // Execute the mapping
            QuadStore result = executor.execute(null);

            // Output the result
            mapping_result = result.toSortedString();
		} catch (Exception e) {
            e.printStackTrace();
			mapping.setStatus(Status.ERROR);
			mapping.setError(e.getMessage());
			return mapping;
		}
        
        String graph = this.writeRDFToGraph(mapping_result);
        mapping.setTargetGraph(graph);
        mapping.setStatus(Status.FINISHED);
		
		return mapping;
	}
	
	/**
	 * Use created RDF via mapping and writes it into the store. Returns the graph URI for later access.
	 * @param rdf String
	 * @return String graph
	 */
	private String writeRDFToGraph(String rdf) {
		return "";
	}
	
	/**
	 * Tries to get a RML mapping from the store and returns it.
	 * Atm I dont know how to insert a ttl file and get it back via Jena
	 * @param mapping Mapping
	 * @return String
	 */
	private String getRMLMapping(Mapping mapping) {
		
		/*
		String queryString = "PREFIX d: <http://dataretrieval.stream-projekt.net/>  SELECT * FROM NAMED <http://dataretrieval.stream-projekt.net/> { GRAPH ?g { ?s ?p ?o . ?s a d:Mapping . ?s d:id \""+id+"\" . ?s d:Repository d:"+repository+" } }" ;
		QueryExecution qexec = QueryExecutionFactory.sparqlService(this.getEndpoint(), queryString, this.getClient());
		Model model = qexec.execConstruct();
		*/
		
		
		
		
		return "ERROR: No mapping";
	}
	
	/**
	 * Calls the NOMAD Archive with the given id and returns the JSON result. If an error occurs then it returns a JSONObject with only the error on the error key.
	 * @param id
	 * @return JSONObject
	 */
	private JSONObject callNOMAD(String id) {
		JSONObject ret = new JSONObject();
		try {

            URL url = new URL("https://nomad-lab.eu/prod/rae/api/archive/"+id);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.connect();

            //Getting the response code
            int responsecode = conn.getResponseCode();

            if (responsecode != 200) {
                throw new RuntimeException("HttpResponseCode: " + responsecode);
            } else {

                String inline = "";
                Scanner scanner = new Scanner(url.openStream());

                //Write all the JSON data into a string using a scanner
                while (scanner.hasNext()) {
                    inline += scanner.nextLine();
                }

                //Close the scanner
                scanner.close();

                //Using the JSON simple library parse the string into a json object
                JSONParser parse = new JSONParser();
                ret = (JSONObject) parse.parse(inline);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Map<String, String> map = new HashMap<String, String>();
            map.put("error", e.getMessage());
            ret = new JSONObject(map);
        }
		return ret;
	}
	
	/**
	 * Defines an HttpClient which does use basic user authentication (digest)
	 * @return HttpClient
	 */
	private HttpClient getClient() {
		Map<String, String> env = System.getenv();
		String username = "test";
		String password = "test";
		String port = "8890";
		if (env.containsKey("SPARQL_USERNAME"))
			username = env.get("SPARQL_USERNAME");
		if (env.containsKey("SPARQL_PASSWORD"))
			password = env.get("SPARQL_PASSWORD");
		if (env.containsKey("SPARQL_PORT"))
			password = env.get("SPARQL_PORT");
		
		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		Credentials unscopedCredentials = new UsernamePasswordCredentials(username, password);
		credsProvider.setCredentials(AuthScope.ANY, unscopedCredentials);
		Credentials scopedCredentials = new UsernamePasswordCredentials(username, password);
		final String host = this.getEndpoint();
		final String realm = "SPARQL Endpoint";
		final String schemeName = "DIGEST";
		AuthScope authscope = new AuthScope(host, Integer.parseInt(port), realm, schemeName);
		credsProvider.setCredentials(authscope, scopedCredentials);
		HttpClient httpclient = HttpClients.custom()
		    .setDefaultCredentialsProvider(credsProvider)
		    .build();
		return httpclient;
	}
}