"""Retrieve GEO data for an experiment, classifying groups by expression data.
"""
import sys
import os
import csv
import collections
import json
import pickle

from Bio import Entrez
import rpy2.robjects as robjects

def main():
    organism = "Mus musculus"
    cell_types = ["proB", "ProB", "pro-B"]
    email = "chapmanb@50mail.com"
    save_dir = os.getcwd()
    exp_data = get_geo_data(organism, cell_types, email, save_dir,
        _is_wild_type)

def _is_wild_type(result):
    """Check if a sample is wild type from the title.
    """
    return result.samples[0][0].startswith("WT")

def get_geo_data(organism, cell_types, email, save_dir, is_desired_result):
    save_file = os.path.join(save_dir, "%s-results.pkl" % cell_types[0])
    #if not os.path.exists(save_file):
    results = cell_type_gsms(organism, cell_types, email)
    test = ""
    #what is all this?
    #takes first result, serializes info using pickle and saves (usually checks if already have results)
    for result in results:
        if is_desired_result(result):
            with open(save_file, "wb") as out_handle:
                pickle.dump(result, out_handle)
                test = result
            break
    #load results
    # with open(save_file, "rb") as save_handle:
    #     result = pickle.load(save_handle)
    result = test
    #print(result)
    #get expression info from the sample in the query result
    exp = result.get_expression(save_dir)
    #?
    for gsm_id, exp_info in exp.items():
        print(gsm_id, exp_info.items()[:5])
    return exp

#main point is to get sample id matching query (GSM#), should be equivalent of searching on the sqllite file
def cell_type_gsms(organism, cell_types, email):
    """Use Entrez to retrieve GEO entries for an organism and cell type.
    """
    Entrez.email = email
    search_term = "%s[ORGN] %s" % (organism, " OR ".join(cell_types))
    print("Searching GEO and retrieving results: %s" % search_term)
    #exit()
    hits = []
    #GDS is geo datasets database
    #https://www.ncbi.nlm.nih.gov/books/NBK25497/table/chapter2.T._entrez_unique_identifiers_ui/
    #search for matching data sets based on query
    handle = Entrez.esearch(db="gds", term=search_term)
    results = Entrez.read(handle)
    
    #go through the data sets that matched
    for geo_id in results['IdList']:
        #get the data set summary by its id
        handle = Entrez.esummary(db="gds", id=geo_id)
        summary = Entrez.read(handle)
        print(summary[0].keys())
        # print(str(summary[0]['summary']).encode('utf8', 'replace'))
        # print(summary[0]['Samples'][0]["Title"])
        # print(summary[0]['Samples'][0]["Accession"])
        exit()

        #end up with summary, just for printing, not really used
        #and title and accession
        #title used as index for dict returned by get_expression method
        #accession is the actual id used to get data
        #GSM# (sample)
        samples = []
        
        #what is this? why is it checking for the cell type name in the title,
        #shouldn't this be handled by the query (which limits cell types)?
        #datasets may have multiple related samples, shouldn't they all still meet the other criteria? not sure
        for sample in summary[0]['Samples']:
            for cell_type in cell_types:
                if sample['Title'].find(cell_type) >= 0:
                    samples.append((summary[0]['Samples'][0]['Title'], "GSM81022"))
                    break
        if len(samples) > 0:
            hits.append(GEOResult(summary[0]['summary'], samples))
    return hits

class GEOResult:
    """Represent a GEO summary with associated samples, getting expression data.
    """
    def __init__(self, summary, samples):
        self.summary = summary
        self.samples = samples

    def __str__(self):
        out = "- %s\n" % self.summary
        for title, accession in self.samples:
            out += " %s %s\n" % (title, accession)
        return out

    #DATA RETREIVAL ENTRYPOINT
    def get_expression(self, save_dir):
        """Retrieve microarray results for our samples mapped to transcript IDs
        """
        results = dict()
        for (title, gsm_id) in self.samples:
            tx_to_exp = self.get_gsm_tx_values(gsm_id, save_dir)
            results[title] = tx_to_exp
        return results

    #MAIN ACTUAL WORK FUNCTION, get_expression just maps the results from here to the sample title
    #what is a transcript?
    def get_gsm_tx_values(self, gsm_id, save_dir):
        """Retrieve a map of transcripts to expression from a GEO GSM file.
        """

        gsm_meta_file = os.path.join(save_dir, "%s-meta.txt" % gsm_id)
        gsm_table_file = os.path.join(save_dir, "%s-table.txt" % gsm_id)


        #just redo everything, want to go through the whole process (tries to used already existing files)
        # if (not os.path.exists(gsm_meta_file) or 
        #         not os.path.exists(gsm_table_file)):
        #first part of the chain, creates metadata and table files for the sample id (GSM#)
        #meta file seems to be in JSON
        #table file nopt actually writting anything... just empty string
        self._write_gsm_map(gsm_id, gsm_meta_file, gsm_table_file)

        #OK, so table stuff doesn't work, but should have a JSON meta file, what's next

        #load the sample metadata saved by r
        with open(gsm_meta_file) as in_handle:
            gsm_meta = json.load(in_handle)
        
        #get the platform id from the sample metadata (samples reference exactly one platform)
        #probe id to biological gene identifiers map
        id_to_tx = self.get_gpl_map(gsm_meta['platform_id'], save_dir)
        
        tx_to_vals = collections.defaultdict(list)

        #well, definitely going to have a problem here since need table file
        #what is this expected to do if it worked?
        with open(gsm_table_file) as in_handle:
            #table is supposed to be a csv
            reader = csv.reader(in_handle, dialect='excel-tab')
            #skip the header in the csv
            reader.next() # header
            #this is where we get the actual values
            #where is it supposed to get these values from???

            #supposed to match first column in sample table (appears to be ID_REF) to ID column in platform, should both be probe ids
            #map each biological gene identifiers in the mapped list to its expression value from the sample data
            for probe_id, probe_val in reader:
                for tx_id in id_to_tx.get(probe_id, []):
                    tx_to_vals[tx_id].append(float(probe_val))
        return tx_to_vals


    #first part of process, handle sample
    def _write_gsm_map(self, gsm_id, meta_file, table_file):
        """Retrieve GEO expression values using Bioconductor, saving to a table.
        """
        
        #getGEO seems to get soft file and deserialize it into a python object
        #assigns fields in soft file to object properties
        #Table(gsm) is then supposed to take this and serialize it into a different format?

        #write.table
        #https://www.rdocumentation.org/packages/utils/versions/3.6.2/topics/write.table
        #deals with data frames
        #Table(gsm) creates an empty data frame? This is why it's writting out an empty string
        #can gsm be used directly? It seems to be one already, also table.write may be able to handle conversion to some extent
        #nope, returns "cannot coerce class 'structure("GSM", package = "GEOquery")' to a data.frame"

        #issue with table, but Meta(gsm) seems to work fine, appears to convert to a dict which can then be serialized to a JSON object
        #serialized metadata written to metadata file with cat

        #no issue with Table, the data set is just empty...

        robjects.r.assign("gsm.id", gsm_id)
        robjects.r.assign("table.file", table_file)
        robjects.r.assign("meta.file", meta_file)
        robjects.r('''
          library(GEOquery)
          library(rjson)
          gsm <- getGEO(gsm.id)
          write.table(Table(gsm), file = table.file, sep = "\t", row.names = FALSE,
                      col.names = TRUE)
          cat(toJSON(Meta(gsm)), file = meta.file)
        ''')



    def get_gpl_map(self, gpl_id, save_dir):
        """Retrieve a map of IDs to transcript information from a GEO GPL file.
        """
        gpl_file = os.path.join(save_dir, "%s-map.txt" % gpl_id)
        if not os.path.exists(gpl_file):
            self._write_gpl_map(gpl_id, gpl_file)
        gpl_map = collections.defaultdict(list)
        with open(gpl_file) as in_handle:
            reader = csv.reader(in_handle, dialect='excel-tab')
            reader.next() # header

            #separates out template ids (RefSeq transcript ids (what is this? the walkthrough seems to refer the them as "biological gene identifiers"))
            #maps this id set to the probe ids (ID column)
            for probe_id, tx_id_str in reader:
                for tx_id in tx_id_str.split(' /// '):
                    if tx_id:
                        gpl_map[probe_id].append(tx_id)
        return dict(gpl_map)

    def _write_gpl_map(self, gpl_id, gpl_file):

        #subset projects the columns ID and RefSeq.Transcript.ID
        #RefSeq.Transcript.ID may not exist, what is this used for?
        #RefSeq.Transcript.ID should be the gene identifier (based on the description at http://bcb.io/2010/01/02/automated-retrieval-of-expression-data-with-python-and-r/)

        """Retrieve GEO platform data using R and save to a table.
        """
        robjects.r.assign("gpl.id", gpl_id)
        robjects.r.assign("gpl.file", gpl_file)
        robjects.r('''
          library(GEOquery)
          gpl <- getGEO(gpl.id)
          Table(gpl)
          gpl.map <- subset(Table(gpl), select=c("ID", "RefSeq.Transcript.ID"))
          write.table(gpl.map, file = gpl.file, sep = "\t", row.names = FALSE,
                      col.names = TRUE)
        ''')



if __name__ == "__main__":
    main(*sys.argv[1:])
