package net.streamproject.dataretrieval;


public class Mapping {

	private final String id;
	private Status status;
	private final DataRepository repository;
	private String error;
	private String targetGraph;
	/**
	 * 
	 * @param id
	 * @param status
	 * @param repository
	 * @param error
	 * @param targetGraph
	 */
	public Mapping(String id, Status status, DataRepository repository, String error, String targetGraph) {
		super();
		this.id = id;
		this.status = status;
		this.repository = repository;
		this.error = error;
		this.targetGraph = targetGraph;
	}
	public Status getStatus() {
		return status;
	}
	public void setStatus(Status status) {
		this.status = status;
	}
	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}
	public String getTargetGraph() {
		return targetGraph;
	}
	public void setTargetGraph(String targetGraph) {
		this.targetGraph = targetGraph;
	}
	public String getId() {
		return id;
	}
	public DataRepository getRepository() {
		return repository;
	}
	
	
}
