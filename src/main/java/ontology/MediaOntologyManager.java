package ontology;

import com.clarkparsia.pellet.owlapiv3.PelletReasoner;
import com.clarkparsia.pellet.owlapiv3.PelletReasonerFactory;
import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.Node;
import org.semanticweb.owlapi.reasoner.NodeSet;
import org.semanticweb.owlapi.reasoner.OWLReasoner;

import java.io.File;
import java.util.*;

/**
 * Created by NK on 2016. 8. 23..
 */
public class MediaOntologyManager {
    public final static void main(String[] args) throws Exception {
//        String owlFilePath = "data/owl/personalMedia_mod.owl";
        String owlFilePath = "data/owl/automated-personalMedia-object-10.owl";

//        reasoningOnotologyUsingPellet(owlFilePath);
        reasoningOntologyUsingHermit("M_",owlFilePath);
    }

    public static List reasoningOnotologyUsingPellet(String className, String filePath) throws Exception {
        System.out.print("Reading file " + filePath+ "...");
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(new File(filePath));

        PelletReasoner reasoner = PelletReasonerFactory.getInstance().createReasoner( ontology );
        System.out.println("done.");

        reasoner.getKB().realize();
//        reasoner.getKB().printClassTree();

        return outputResult(manager, reasoner, className);
    }
    public static List reasoningOntologyUsingHermit(String classNameIRI, String filePath) throws Exception {
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        OWLOntology ontology = manager.loadOntologyFromOntologyDocument(new File(filePath));

        Reasoner reasoner = new Reasoner(ontology);
//        System.out.println(reasoner.isConsistent());

        return outputResult(manager, reasoner, classNameIRI);
    }

    protected static List outputResult(OWLOntologyManager manager, OWLReasoner reasoner, String classNameIRI){
        // create property and resources to query the reasoner
        OWLClass automadedActivityClass = manager.getOWLDataFactory().getOWLClass(IRI.create(classNameIRI));

        // get all instances of automadedActivity class
        NodeSet<OWLNamedIndividual> individuals = reasoner.getInstances( automadedActivityClass, false);

//        System.out.println("총 [ "+ classNameIRI + "] Infered Instance 갯수 : " + individuals.getNodes().size());

        List<String> inferedInstanceArray = new ArrayList<String>();
        for(Node<OWLNamedIndividual> sameInd : individuals) {
            inferedInstanceArray.add("<"+sameInd.getEntities().iterator().next().getIRI().toString()+">");
        }
        Collections.sort(inferedInstanceArray,new NameAscCompare());

        return inferedInstanceArray;
    }

    /**
     * 이름 오름차순
     * @author falbb
     *
     */
    static class NameAscCompare implements Comparator<String> {
        /**
         * 오름차순(ASC)
         */
        @Override
        public int compare(String arg0, String arg1) {
            // TODO Auto-generated method stub
            return arg0.compareTo(arg1);
        }

    }
}
