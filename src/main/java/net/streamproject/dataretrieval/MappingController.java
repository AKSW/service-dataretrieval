package net.streamproject.dataretrieval;

import java.util.Map;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
	
	private Mapping readMapping(String id, DataRepository repository) {
		Mapping mapping = new Mapping(id, Status.UNKNOWN, repository, "", "");
		
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
		
		return mapping;
	}
	
	@GetMapping("/startExecution")
	public Mapping startExecution(@RequestParam(value = "id", defaultValue = "") String id, @RequestParam(value = "repository", defaultValue = "NOMAD") DataRepository repository) {
		// check RDF graph if the id on the repository is known and what the status is
		Mapping mapping = this.readMapping(id, repository);
		if (mapping.getStatus() == Status.RUNNING)
			return mapping;
		
		//TODO <- if it is not running, then get the data and the mappings, start RMLMapper with it, write it into a specific graph and return the graph URI
		
		
		
		return mapping;
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
