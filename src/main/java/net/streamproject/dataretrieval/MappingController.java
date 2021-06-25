package net.streamproject.dataretrieval;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.web.bind.annotation.CrossOrigin;
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

@CrossOrigin
@RestController
public class MappingController {
	private final String endpoint = "http://localhost:8890/sparql/";
	private String getEndpoint() {
		Map<String, String> env = System.getenv();
		if (env.containsKey("SPARQL_URL"))
			return env.get("SPARQL_URL");
		return endpoint;
	}
	private static String baseURI = "http://dataretrieval.stream-dataspace.net/";

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
	
	/**
	 * Does read the mapping with Jena and return the complete mapping object
	 * @param id
	 * @param repository NOMAD or DSMS
	 * @return Mapping
	 */
	private Mapping readMapping(String id, DataRepository repository) {
		Mapping mapping = new Mapping(id, Status.UNKNOWN, repository, "", "");
		
		System.out.println(this.getEndpoint());
		
		// See https://jena.apache.org/documentation/query/app_api.html
		try {
			// check RDF graph if the id on the repository is known and what the status is
			String queryString = "PREFIX d: <"+MappingController.baseURI+">  SELECT * FROM NAMED <"+MappingController.baseURI+"> { GRAPH ?g { ?s ?p ?o . ?s a d:Mapping . ?s d:id \""+id+"\" . ?s d:Repository d:"+repository+" } }" ;
			QueryExecution qexec = QueryExecutionFactory.sparqlService(this.getEndpoint(), queryString, this.getClient());
			ResultSet results = qexec.execSelect() ;
		    for ( ; results.hasNext() ; )
		    {
		      QuerySolution soln = results.nextSolution() ;
		      RDFNode x = soln.get("s") ;       // Get a result variable by name.
		      Resource r = soln.getResource("p") ; // Get a result variable - must be a resource
		      RDFNode l = soln.get("o") ;   // Get a result variable by name.
		      System.out.println(x.toString() + " " + r.toString() + " " + l.toString() + " .");
		      
		      switch (r.toString().substring(MappingController.baseURI.length())) {
			      case "Status":
			    	  mapping.setStatus(Status.valueOf(l.toString()));
			    	  break;
			      case "Error":
			    	  mapping.setError(l.toString());
			    	  break;
			      case "TargetGraph":
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
	
	/**
	 * Does execute a complete mapping.
	 * That means that the data from the data repository is read, mapped to RDF via a RML mapping saved in a RDF store. At the end the rest is stored in the store and the mapping object is returned.
	 * @param id
	 * @param repository NOMAD or DSMS
	 * @return Mapping on error the execution stops and the mapping object is returned.
	 */
	@GetMapping("/startExecution")
	public Mapping startExecution(@RequestParam(value = "id", defaultValue = "") String id, @RequestParam(value = "repository", defaultValue = "NOMAD") DataRepository repository) {
		// check RDF graph if the id on the repository is known and what the status is
		Mapping mapping = this.readMapping(id, repository);
		if (mapping.getStatus() == Status.RUNNING)
			return mapping;
		else if (mapping.getStatus() == Status.FINISHED)
			return mapping;
		else {
			//clean error if set and set to running
			mapping.setError("");
			mapping.setStatus(Status.RUNNING);
			this.writeMapping(mapping);
		}
		
		// get the data and the mappings
		if (mapping.getRepository() == DataRepository.DSMS) {
			mapping.setStatus(Status.ERROR);
			mapping.setError("DSMS is not yet supported for startExecution. Reason is missing JSON API.");
			this.writeMapping(mapping);
			return mapping;
		}
		JSONObject nomad_archive_result = this.callNOMAD(id);
		if (nomad_archive_result.containsKey("error")) {
			mapping.setStatus(Status.ERROR);
			mapping.setError(nomad_archive_result.get("error").toString());
			this.writeMapping(mapping);
			return mapping;
		}
		System.out.println("NOMAD data:\n"+nomad_archive_result.toJSONString().substring(0, 400) + " ...");
		File tempFile; // save json in file for RMLMapper
		try {
			tempFile = File.createTempFile("NOMAD-", ".json");
			tempFile.deleteOnExit();
			FileWriter myWriter = new FileWriter(tempFile);
		    myWriter.write(nomad_archive_result.toJSONString());
		    myWriter.close();
		} catch (Exception e) {
			e.printStackTrace();
			mapping.setStatus(Status.ERROR);
			mapping.setError(e.getMessage());
			this.writeMapping(mapping);
			return mapping;
		}
		String rml_mapping = this.getRMLMapping(mapping);
		if (rml_mapping.startsWith("ERROR:")) {
			mapping.setStatus(Status.ERROR);
			mapping.setError(rml_mapping);
			this.writeMapping(mapping);
			return mapping;
		}
		// replace placeholder with path to json file
		rml_mapping = rml_mapping.replace("RMLMAPPER-FILE-URL_REPLACEMENT", tempFile.getAbsolutePath());
		System.out.println("RML:\n"+rml_mapping);

		// start RMLMapper with it
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
            mapping_result = result.toSortedString().replaceAll(" null", " .");
		} catch (Exception e) {
            e.printStackTrace();
			mapping.setStatus(Status.ERROR);
			mapping.setError(e.getMessage());
			this.writeMapping(mapping);
			return mapping;
		}
        System.out.println("mapping result:\n"+mapping_result);
        
        // execute Lime and add the sameAs triples
        String sameAs = "";
        try {
        	sameAs = this.executeLimes();
		} catch (IOException e) {
			e.printStackTrace();
		}
        if (sameAs.length() > 0) {
        	String sameAs_without_prefixes = "";
        	String[] lines = sameAs.split(System.getProperty("line.separator"));
        	//System.out.println("sameAs: " + sameAs + System.getProperty("line.separator") + "lines number: " + lines.length);
        	int i = 0;
        	for (; i < lines.length; i++) {
        		if (lines[i].startsWith("@prefix"))
        			continue;
        		sameAs_without_prefixes += System.getProperty("line.separator") + lines[i];
        	}
        	System.out.println("sameAs_without_prefixes: " + sameAs_without_prefixes);
        	mapping_result += System.getProperty("line.separator") + sameAs_without_prefixes;
        }
        
        // write it into a specific graph and return the graph URI
        String graph = this.writeRDFToGraph(mapping_result, mapping.getId());
        if (graph.length() < 1) {
        	mapping.setStatus(Status.ERROR);
			mapping.setError("target graph could not be created");
			this.writeMapping(mapping);
			return mapping;
        }
        mapping.setTargetGraph(graph);
        mapping.setStatus(Status.FINISHED);
        
        this.writeMapping(mapping);
        
		
		return mapping;
	}
	
	/**
	 * Does execute LIMES with the given configuration file.
	 * Atm every call writes into the same output file.
	 * @return String turtle
	 * @throws IOException
	 */
	private String executeLimes() throws IOException {
		String config = "example_limes_config.xml";
		Map<String, String> env = System.getenv();
		if (env.containsKey("LIMES_CONFIG"))
			config = env.get("LIMES_CONFIG");
		
		// TODO copy config file and replace accept.nt in order to answer multiple requests at once
		// Run a java app in a separate system process
		Process proc = Runtime.getRuntime().exec("java -jar limes.jar " + config);
		// Then retrieve the process output
		InputStream in = proc.getInputStream();
		InputStream err = proc.getErrorStream();
		
		String output = IOUtils.toString(in, StandardCharsets.UTF_8) + "; Error: "+IOUtils.toString(err, StandardCharsets.UTF_8);
		System.out.println(output);
		
		String accepted = Files.readString(Paths.get("./accepted.nt"), StandardCharsets.US_ASCII);
		
		return accepted;
	}
	
	/**
	 * This will write the mapping into the graph in order that the information is available.
	 * It will erase the existing mapping beforehand. 
	 * @param mapping Mapping
	 */
	private void writeMapping(Mapping mapping) {
		String query_string = "PREFIX d: <"+MappingController.baseURI+">\n"
				+ "DELETE { GRAPH <"+MappingController.baseURI+"> {\n"
				+ "  ?s ?p ?o } } where {\n"
				+ "?s ?p ?o . filter ( ?s = <"+MappingController.baseURI+mapping.getId()+">) \n"
				+ " }\n"
				+ "INSERT data into  <"+MappingController.baseURI+"> { <"+MappingController.baseURI+mapping.getId()+"> a d:Mapping .\n"
				+ "<"+MappingController.baseURI+mapping.getId()+">  d:id \""+mapping.getId()+"\" .\n"
				+ "<"+MappingController.baseURI+mapping.getId()+">  d:Repository d:"+mapping.getRepository().toString()+" .\n"
				+ "<"+MappingController.baseURI+mapping.getId()+">  d:TargetGraph <"+mapping.getTargetGraph()+"> .\n"
				+ "<"+MappingController.baseURI+mapping.getId()+">  d:Error \""+mapping.getError()+"\" .\n"
				+ "<"+MappingController.baseURI+mapping.getId()+">  d:Status \""+mapping.getStatus().toString()+"\". }";
		System.out.println("Update string:\n"+query_string);
		
		// Jena and virtuoso do not work together on SPARQL update requests, thus the virtuoso sparql endpoint has to be called directly
		try {

            URL url = new URL(this.getEndpoint()+"?query="+URLEncoder.encode(query_string, StandardCharsets.UTF_8.toString())+"&format=text%2Fturtle&run=+Run+Query+");

            HttpClient client = this.getClient();
            HttpGet get = new HttpGet(url.toString());
            HttpResponse response = client.execute(get);

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("HttpResponseCode: " + response.getStatusLine().toString());
            } else {

                System.out.println("SPARQL Update result:\n"+response.getEntity().toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	/**
	 * Use created RDF via mapping and writes it into the store. Returns the graph URI for later access.
	 * @param rdf String
	 * @return String graph
	 */
	private String writeRDFToGraph(String mapping_result, String id) {
		String graph = MappingController.baseURI+"results/"+id;
		String query_string = "CLEAR GRAPH <"+graph+"> INSERT data into  <"+graph+"> { "+mapping_result+" }";
		System.out.println("Insert string:\n"+query_string);
		
		// Jena and virtuoso do not work together on SPARQL update requests, thus the virtuoso sparql endpoint has to be called directly
		try {

            URL url = new URL(this.getEndpoint()+"?query="+URLEncoder.encode(query_string, StandardCharsets.UTF_8.toString())+"&format=text%2Fturtle&run=+Run+Query+");

            HttpClient client = this.getClient();
            HttpGet get = new HttpGet(url.toString());
            HttpResponse response = client.execute(get);

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("HttpResponseCode: " + response.getStatusLine().toString());
            } else {

                System.out.println("SPARQL Insert result:\n"+response.getEntity().toString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
		
		return graph;
	}
	
	/**
	 * Tries to get a RML mapping from the store and returns it.
	 * Atm I dont know how to insert a ttl file and get it back via Jena
	 * @param mapping Mapping
	 * @return String
	 */
	private String getRMLMapping(Mapping mapping) {
		
		// TODO read graph URI from mapping
		String queryString = "CONSTRUCT { ?s ?p ?o } WHERE { GRAPH <"+MappingController.baseURI+"mappings/test/> { ?s ?p ?o } . }" ;
		QueryExecution qexec = QueryExecutionFactory.sparqlService(this.getEndpoint(), queryString, this.getClient());
		Model model = qexec.execConstruct();
		
		OutputStream outputStream = new ByteArrayOutputStream();
		RDFDataMgr.write(outputStream, model, Lang.TURTLE);
		String rdf = new String(((ByteArrayOutputStream) outputStream).toByteArray());
		
		qexec.close();
		
		if (rdf.length() < 3)
			return "ERROR: No mapping";
		return rdf;
	}
	
	/**
	 * Calls the NOMAD Archive with the given id and returns the JSON result. If an error occurs then it returns a JSONObject with only the error on the error key.
	 * @param id
	 * @return JSONObject
	 */
	private JSONObject callNOMAD(String id) {
		JSONObject ret = new JSONObject();
		try {
			// use calc_id and upload_id
			String upload_id = id.split("/")[0];
			String calc_id = id.split("/")[1];
            URL url = new URL("http://nomad-lab.eu/prod/rae/api/archive/"+upload_id+"/"+calc_id);

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
			port = env.get("SPARQL_PORT");
		
		CredentialsProvider credsProvider = new BasicCredentialsProvider();
		Credentials unscopedCredentials = new UsernamePasswordCredentials(username, password);
		credsProvider.setCredentials(AuthScope.ANY, unscopedCredentials);
		Credentials scopedCredentials = new UsernamePasswordCredentials(username, password);
		final String host = this.getEndpoint();
		final String realm = "SPARQL Endpoint";
		final String schemeName = "Digest";
		AuthScope authscope = new AuthScope(host, Integer.parseInt(port), realm, schemeName);
		credsProvider.setCredentials(authscope, scopedCredentials);
		HttpClient httpclient = HttpClients.custom()
		    .setDefaultCredentialsProvider(credsProvider)
		    .build();
		return httpclient;
	}
}
