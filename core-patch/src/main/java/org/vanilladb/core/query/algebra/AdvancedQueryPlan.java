// YAHUI: Plan
package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.distfn.DistanceFn;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;

public class AdvancedQueryPlan implements Plan {
    // private TableInfo ti;
    // private TableStatInfo si;

    private List<Plan> subPlans = new ArrayList<Plan>();
    private List<Scan> subScans = new ArrayList<Scan>();
    private List<DistanceFn> embFields = new ArrayList<DistanceFn>();
    private Set<String> projectFields = new HashSet<String>();
    private int curCluster = 0;

    public AdvancedQueryPlan(List<Plan> sPlans, List<DistanceFn> embFields, Set<String> projectFields) {
        subPlans.addAll(sPlans);
        this.embFields.addAll(embFields);
        this.projectFields.addAll(projectFields);
        curCluster = 0;
    }


    @Override
    public Scan open() {
        // TODO Auto-generated method stub
        for (Plan p : subPlans) {
            subScans.add(p.open());
        }
        return new AdvancedQueryScan(subScans, embFields);
        
    }

    @Override
    public Histogram histogram() {
        // TODO Auto-generated method stub
        return subPlans.get(curCluster).histogram();
    }

    @Override
    public long blocksAccessed() {
        // TODO Auto-generated method stub
        long result = 0;
        for (Plan p : subPlans) {
            result += p.blocksAccessed();
        }
        return result;
    }
    
    @Override
    public long recordsOutput() {
        // TODO Auto-generated method stub
        long result = 0;
        for (Plan p : subPlans) {
            result += p.recordsOutput();
        }
        return result;
    }

    @Override
    public Schema schema() {
        // TODO Auto-generated method stub
        return subPlans.get(curCluster).schema();
    }

    public void setCluster(int cluster) {
        curCluster = cluster;
    }
}
