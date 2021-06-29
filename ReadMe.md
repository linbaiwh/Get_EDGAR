## Description

command-line program to get files from EDGAR and select paragraphs including specified keywords
```bash
python app.py purpose tag start_year end_year form_types [options]
```
## Positional Arguments

| Argument | Description|
| -------  | -------------------|
| purpose | The purpose of the command<br>Must be one of the following options:<br>- **_dl_index_**: To download EDGAR Index<br>- **_index_evtfilter_**: To keep the filings after a specific period of the event dates<br> - **_file_info_**: To add filing information to index<br>- **_finfo_evtfilter_**: To keep the filings after a specific period of the event dates<br> - **_extract_items_**: To select specific **8-K** filings<br>- **_text_info_**: To add text information to the file information|
|tag|The tag associated with the command<br>Decides the folder and variable name for text_info<br>Must be a string|
|start_year|The start year to scrap EDGAR<br>Must be a integer|
|end_year|The end year to scrap EDGAR<br>Must be a integer|
|form_types|The forms to scrap, need to comform with EDGAR<br>Index file. Case sensitive. <br>Can be multiple strings<br>Each string needs to be wrapped by ''<br>Example: '8-K' 'DEF 14A' '10-K'|

## Options
|Option|Description|
|------|-----------|
|--topfolder|The top-level folder to save all the output files<br>Need to be absolute valid path<br>If not specified, all the output files will be saved to .\data|
|--cikfile|The csv file containing all the ciks to download EDGAR Index<br>Only applies to purpose **dl_index**<br>The variable name for the cik should be **CIK**<br>No zeros at the beginning of the CIK |
|--cikset|The list containing all the ciks to download EDGAR Index<br>Only applies to purpose **dl_index**<br>If specified, overwrite the `--cikfile` option<br>Format: 'cik1 cik2 cik3'<br>No zeros at the beginning of the CIK|
|--evtfilter|Whether to keep filings after a specific period of the event dates<br> Bool <br> If True, the event dates file is the same as the cikfile |
|--mperiods|The months after the event dates<br> Applies to purpose **index_evtfilter** & **finfo_evtfilter**<br> Must be an integer 
|--itemfile|The txt file containing the key words to select specific **8-K** filings<br>Applies to purpose **extract_items** & **text_info**<br>|
|--itemset|The list containing all the key words to select specific **8-K** filings<br>Applies to purpose **extract_items** & **text_info**<br>If specified, overwrite the `--itemfile` option<br>Each key word should be wrapped with ''<br>Format: 'Key word 1' 'Key word 2'<br>The key words are case sensitive|
|--filterfile|The txt file containning the key words to find specific information for all links (files) in a SEC filing<br>Only applies to purpose **text_info**|
|--excludesfile|The txt file containning the key words to exclude specific information for all links (files) in a SEC filing <br>Only applies to purpose **text_info**|
|--save_txt|Whether to save the filtered paragraphs<br>Must be one of the following options:<br>- **_True_**: To save text information for all the SEC Filings and with filtered paragraphs<br>- **_False_**: Do not save any text information
|--split|Number to split each input csv file<br>Applies to purpose **file_info** & **text_info**<br>Default value is 0|
