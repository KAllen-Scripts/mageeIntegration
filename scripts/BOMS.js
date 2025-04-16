let common = require('../common.js');
const convertCSV = require("json-2-csv");
const fs = require('fs');
const csv = require('fast-csv');
const path = require('path');
process.chdir(__dirname);
let bomFolderPath = '../files - new/BOM/details';
let bomDetailsFolderPath = '../files - new/RM';
let bomCostsFolderPath = '../files - new/cost/Addtional_Element';
let supplierSheet = '../files - new/sage stuff/suppliers.csv';
let productFolderPath = '../files - new/product';
let bomRawCostsFolderPath = '../files - new/cost/header';

let supplierLookup = {};
let BOMS = {};
let BomDetailsLookup = {};
let itemList = {};
let itemTypeLookup = {};
let itemTypesToMake = [];
let BOMNumbers = {};
let simpleProducts = {};
let allCurrentBoms = [];
let suppliers = {};
let manufacturers = {};
let BOMRawCosts = {};
let itemIDLookup = {};
let itemListLookup = {};
let additionalCosts = {};

const defaultTaxClass = 'c25a24dd-5ff3-43bf-be8c-348712dfc64d'

let tempList = ['STTTRS_43633','STTTRS_62059','STTTRS_62060','STTTRS_62061','STTTRS_62062','STTTRS_62063','STTTRS_62075','STTTRS_62076','PECOAT_55020','PECOAT_55021','PECOAT_55161','PECOAT_55162','PECOAT_55163','CLSJKT_55025','CLSJKT_55026','CLSJKT_55075','CLSJKT_55100','CLSJKT_55180','DUCWTRS_61849','DUCWTRS_62001','DUCWTRS_62003','DUCWTRS_62004','DUCWTRS_62005','DUCWTRS_62006','DUCWTRS_62011','DUCWTRS_62012','DUCWTRS_62050','DUCWTRS_62051','DUCWTRS_62053','DUCWTRS_62054','DUCWTRS_62081','DUCWTRS_62082','DUCWTRS_62083','DUCWTRS_62085','DUCWTRS_62086','DUCWTRS_62087','DUCWTRS_62088','DUCWTRS_62106','DUCWTRS_62107','DUCWTRS_62108','DUCWTRS_62109','DUCWTRS_62110','CATWTRS_61810','CATWTRS_61814','CATWTRS_61849','CATWTRS_62004','CATWTRS_62005','CATWTRS_62006','CATWTRS_62053','CATWTRS_62054','CATWTRS_62078','CATWTRS_62079','CATWTRS_62080','CATWTRS_62082','CATWTRS_62083','CATWTRS_62085','CATWTRS_62086','CATWTRS_62087','CATWTRS_62088','CATWTRS_62089','CATWTRS_62106','CATWTRS_62107','CATWTRS_62108','CATWTRS_62109','CATWTRS_62110','COCSH_80100','COCSH_80101','COCSH_80103','COCSH_80169','COCSH_80289','COCSH_80290','CL2PC_43585','CACKW_80035','CACKW_80037','CACKW_80039','CACKW_80116','CACKW_81012','CACKW_81013','CACKW_81014','CACKW_81015','CACKW_81052','CACKW_81053','CACKW_81054','CACKW_81055','CACKW_81091','CACKW_81092','CACKW_81093','CACKW_81094','CACKW_81095','CACKW_89942','VACKW_80174','VACKW_80175','VACKW_89805','LUCKW_80117','LUCKW_80118','LUCKW_80119','LUCKW_80120','LUCKW_80121','LUCKW_80176','LUVKW_80117','LUVKW_80118','LUVKW_80119','LUVKW_80120','LUVKW_80121','LUVKW_80176','LUQKW_80117','LUQKW_80118','LUQKW_80119','LUQKW_80120','LUQKW_80121','LUQKW_80176','CICKW_80122','CICKW_80123','CICKW_80124','CICKW_80125','CICKW_80126','CICKW_81067','CICKW_81068','CICKW_81069','CIPSKW_80122','CIPSKW_80123','CIPSKW_80124','CIPSKW_80125','CIPSKW_80126','CITKW_80122','CITKW_80123','CITKW_80124','CITKW_80125','CITKW_80126','CITKW_81067','CITKW_81068','CITKW_81069','FIQKW_80128','FIQKW_80129','FIQKW_80130','FIQKW_80131','FICKW_80128','FICKW_80129','FICKW_80130','FICKW_80131','OIQKW_80132','OIQKW_80133','OIQKW_80134','OITKW_80132','OITKW_80133','OITKW_80134','BLCKW_80138','BLCKW_80139','MESJKT_55006','MESJKT_55007','COCOAT_55002','COCOAT_55003','COCOAT_55167','ERCOAT_54014','ERCOAT_55002','EDCOAT_54367','EDCOAT_55000','EDCOAT_55040','EDCOAT_55165','CKCOAT_55040','CKCOAT_55166','FICOAT_55004','FICOAT_55041','FICOAT_55166','GLJKT_54990','GLJKT_54997','TATO_70004','TATO_70005','TATO_70006','TATO_70025','TATO_70026','TATO_80154','TATO_80155','TATO_80156','TATO_80158','TATO_80164','TATO_80238','REDR_70000','REDR_70027','REDR_80148','REDR_80151','REDR_80157','REDR_80243','SUDR_62075','SUDR_70001','SUDR_70003','SUDR_80154','JACKW_80122','JACKW_80123','JACKW_80125','JACKW_80126','JACKW_80127','JACKW_81105','JACKW_81106','JACKW_81112','JAVKW_80122','JAVKW_80123','JAVKW_80125','JAVKW_80126','JAVKW_80127','JACCKW_80122','JACCKW_80123','JACCKW_80125','JACCKW_80126','JACCKW_80127','JACCKW_81008','JACCKW_81010','JACCKW_81011','DETKW_80135','DETKW_80136','DETKW_80137','DECKW_80135','DECKW_80136','DECKW_80137','EDCOATNS_54361','DEMACNS_53866','GLJKTNS_54077','GLJKTNS_55211','LILEHWBLZ_27718','LILEHWBLZ_52545','CACOAT_54931','CACOAT_55022','POMAC_54781','POMAC_55039','POMAC_55185','POMAC_55186','ROJKT_54077','ROJKT_54973','ROJKT_55045','ROJKT_55078','LICBLZNS_8051','LIDBBLZNS_8051','SSMABLZ_8051','MABLZNS_426','MABLZNS_2240','MABLZNS_7480','MABLZNS_8128','MABLZNS_26830','SSMMCLBLZ_41662','SSMMCLBLZ_41943','SSMMCLBLZ_43561','SSMMCLBLZ_43562','SSMMCLPTRS_41662','SSMMCLPTRS_41943','SSMMCLPTRS_43561','SSMMCLPTRS_43562','SSMMCLWC_41662','SSMMCLWC_43561','SSMMCLWC_43562','SSMMTOBLZ_43560','SSMMTOBLZ_43561','SSMMTOBLZ_43562','SSMMTOPTRS_43560','SSMMTOPTRS_43561','SSMMTOPTRS_43562','SSMMTOWC_43560','SSCRBLZ_431','PCBLZNS_431','PCWCNS_431','SSMOCOAT_3539','MOBLZNS_3539','SCKI2PC_41840','SCKI2PC_41842','SCKI2PC_43532','SCKI2PC_60340','SCKI2PC_60341','SCKI2PC_60342','SCOL2PC_41840','SCOL2PC_41842','SCOL2PC_43532','SCOL2PC_60340','SCOL2PC_60341','SCOL2PC_60342','MMLOBLZ_52716','MMLOWC_52716','ACLIBLZ_4678','MBLIHWBLZ_54797','MBLIHWBLZ_54798','SCPO2PC_41840','SCPO2PC_41842','SCPO2PC_43532','SCPO2PC_60340','SCPO2PC_60342','MFTSH_87125','MFTSH_87126','MFTSH_87130','MFCSH_87125','MFCSH_87126','MFCSH_87130','MFTDSH_87125','MFTDSH_87126','MFCDSH_87125','MFCDSH_87126','KISJKT_55004','KISJKT_55178','KISJKT_89970','MAPSKW_80049','MAPSKW_81008','MAPSKW_81009','MAPSKW_81010','OLCKW_81025','OLCKW_81026','SACKW_81016','SACKW_81017','POTWSH_89810','POTWSH_89947','AOBLZ_55102','AOBLZ_55103','AISH_80200','AISH_80243','LOWSH_89215','PATO_70002','PATO_70005','TILTS_80515','MACKW_80049','MACKW_81008','MACKW_81009','MACKW_81010','SSLIHWBLZ_55054','SSLIHWBLZ_55055','SSLIHWBLZ_55055A','SSLIHWBLZ_55054A','SITO_70002','OLQKW_81025','OLQKW_81026','POTSH_80204','POTSH_80205','POTSH_80206','POTSH_80207','POTSH_80278','POTSH_80279','POTSH_80290','POTSH_89809','POTSH_89810','DOCAP_4678','DOCAP_51580','DOCAP_51600','DOCAP_53897','DOCAP_53899','DOCAP_55402','DOCAP_55423','DOCAP_55450','DOCAP_55451','DOCAP_55452','DOCAP_55473','DOCAP_59958','DOCAP_89959','DOCAP_89964','DOCAP_89966','BACAP_53897','BACAP_53899','BACAP_54324','BACAP_55402','BACAP_55423','BACAP_55450','BACAP_55451','BACAP_55452','BACAP_89966','COSK_70000','COSK_70003','COSK_70004','COSK_80157','MOWCNS_3539','MOWCNS_3546','TITRS_43633','TITRS_55101','TITRS_62059','TITRS_62060','TITRS_62061','TITRS_62062','TITRS_62063','TITRS_62075','TITRS_62076','TITRS_80148','BLZTEST_55112','GLYHWWC_55054','GLYHWWC_55055','GLYHWWC_55450','GLYHWWC_55451','GLYHWWC_55452','GLYHWWC_55054A','GLYHWWC_55055A','LIBLZNS_426','LIBLZNS_2240','LIBLZNS_7480','LIBLZNS_8128','LIBLZNS_26830','LICPHWTRS_55054','LICPHWTRS_55055','LICPHWTRS_55450','LICPHWTRS_55451','LICPHWTRS_55452','LICPHWTRS_55055A','LICPHWTRS_55054A','LIHWBLZ_25654','LIHWBLZ_52021','LIHWBLZ_55054','LIHWBLZ_55055','LIHWBLZ_55450','LIHWBLZ_55451','LIHWBLZ_55452','LIHWBLZ_55055A','LIHWBLZ_55054A','MMHIBLZ_24759','MMHIBLZ_55064','SSBRBLZ_24759','SSLIBLZ_8051','SSMMFPDTRS_5281','SSMMFPDTRS_41662','SSTARWC_24759','SSMMFIDBLZ_5281','SSMMFIDBLZ_41662','SSMMLPDTRS_5281','SSMMLPDTRS_41662','MAJKT_55200','MAJKT_55201','CLCKW_81077','CLCKW_81078','DABKW_81060','DABKW_81061','SETKW_81073','OSTKW_81065','OSTKW_81066','OSQKW_81065','OSQKW_81066','KICWSH_80226','KICWSH_89215','KICWSH_89220','KICWSH_89222','KICWSH_89809','KICWSH_89810','KICWSH_89947','KICWSH_89948','KICWSH_89949','SSMOSTRS_3535','SSMOSTRS_3539','DUCWNCTRS_61817','DUCWNCTRS_62011','DUCWNCTRS_62012','COCWSH_80226','COCWSH_89215','COCWSH_89220','COCWSH_89809','COCWSH_89810','COCWSH_89947','COCWSH_89948','COCWSH_89949','SSARWC_431','AIWSH_80294','AIWSH_80295','AIWSH_89215','AIWSH_89220','AIWSH_89222','ANTKW_81064','ANTKW_81065','ANTKW_81066','MEVKW_81074','MEVKW_81075','GLYSLWC_41053','GLYSLWC_43589','CLLSJKT_55162','CLLSJKT_55206','HEVKW_81062','HEVKW_81063','PICKW_81076','PICKW_81077','SSMMLIPTRS_15611','SSMMLIPTRS_17741','SSMMLIPTRS_41803','SSMMLIPTRS_41943','SSMMLIPTRS_43314','SSMMLIPTRS_43561','SSMMLIPTRS_43562','EMCOAT_54099','EMCOAT_54768','EMCOAT_55018','EMCOAT_55019','EMCOAT_55151','EMCOAT_55156','EMCOAT_55157','EMCOAT_55161','RISSH_80234','SUSSH_80514','SUSSH_80521','GLQKW_81079','GLQKW_81080','GCTESTBLZ_54385','SSMMLIWC_15611','SSMMLIWC_17741','SSMMLIWC_41803','SSMMLIWC_43561','SSMMLIWC_43562','SSMMLIBLZ_15611','SSMMLIBLZ_17741','SSMMLIBLZ_41803','SSMMLIBLZ_41943','SSMMLIBLZ_43314','SSMMLIBLZ_43561','SSMMLIBLZ_43562','JOCOAT_54738','JOCOAT_54766','JOCOAT_54767','JOCOAT_54784','JOCOAT_55150','JOCOAT_55156','JOCOAT_55160','TC150X200_30000','TC150X200_30001','TC150X200_30002','TC150X200_30003','TC150X200_30004','TC150X200_30008','TC150X200_30009','TC150X200_30010','TR45X200_30000','TR45X200_30001','TR45X200_30002','TR45X200_30003','TR45X200_30004','TR45X200_30008','TR45X200_30009','TR45X200_30010','NAP45X45_30000','NAP45X45_30001','NAP45X45_30002','NAP45X45_30003','NAP45X45_30004','NAP45X45_30008','NAP45X45_30009','NAP45X45_30010','DUCWTRSNS_61810','DUCWTRSNS_61814','DUCWTRSNS_62106','SADR_55416','SADR_70028','SADR_70029','SADR_70040','SADR_70041','MTOSISJKT_55421','MTOSASJKT_00001','MTOSHBLZ_00003','MTOKNTRS_00003','MTOTRTRS_00004','MTOTRCTRS_00005','MTOVSDBLZ_00007','MTOTSDBLZ_00009','LIGIL_54973','LIGIL_55075','LIGIL_55078','LICPTRS_443','LICPTRS_445','LICPTRS_446','LICPTRS_3530','LICPTRS_3889','LICPTRS_3898','LICPTRS_3899','LICPTRS_7293','LICPTRS_9254','LICPTRS_37634','LICPTRS_37635','LICPTRS_39501','LICPTRS_39502','LICPTRS_39571','LICPTRS_41152','LICPTRS_41153','LICPTRS_43633','LICPTRS_51038','LICPTRS_51040','LICPTRS_60932','LICPTRS_60934','LICPTRS_60951','LICPTRS_61310','LICPTRS_61327','LICPTRS_61752','LICPTRS_62059','LICPTRS_62060','LICPTRS_62061','LICPTRS_62062','LICPTRS_62063','LICPTRS_62100','LICPTRS_62102','LICSTRS_443','LICSTRS_445','LICSTRS_446','LICSTRS_3530','LICSTRS_3889','LICSTRS_3898','LICSTRS_3899','LICSTRS_3900','LICSTRS_7293','LICSTRS_9254','LICSTRS_37634','LICSTRS_37635','LICSTRS_38540','LICSTRS_38550','LICSTRS_39501','LICSTRS_39502','LICSTRS_39571','LICSTRS_41152','LICSTRS_41153','LICSTRS_51038','LICSTRS_51040','LICSTRS_60932','LICSTRS_60934','LICSTRS_60951','LICSTRS_60955','LICSTRS_61310','LICSTRS_61744','LICSTRS_61752','LICSTRS_62059','LICSTRS_62060','LICSTRS_62061','LICSTRS_62062','LICSTRS_62063','KLJKT_55024','KLJKT_55043','KLJKT_55176','KLJKT_55204','KLJKT_55207','MMARBLZ_55422','FIBLZ_43653','FIBLZ_43656','FIBLZ_55010','FIBLZ_55011','MTOARCOAT_55155','MTOARCOAT_55163','SOCOAT_55158','SOCOAT_55159','MMOBBLZ_24759','MMOBBLZ_52715','MMOBBLZ_55424','MMARWC_24759','MMARWC_52715','MMARWC_55064','MMARWC_55422','MMARWC_55424','MMARWC_92025','MEWPJ_61796','MEWPJ_61858','MEWPJ_61927','WOPJ_80243','WOWPJ_61796','WOWPJ_61858','WOWPJ_61927','LITOBLZ_55106','LITOBLZ_55410','LITOBLZ_55453','LITOBLZ_55457','LITOBLZ_55458','LITOBLZ_55459','LITOBLZ_55460','LITOBLZ_55463','LITOBLZ_55464','LITOBLZ_55465','KEJKT_54077','KEJKT_55179','KEJKT_55187','KEJKT_55208','KEJKT_55210','EABLZ_55014','EABLZ_55029','EABLZ_55114','EABLZ_55403','EABLZ_55412','EABLZ_55453','EABLZ_55465','PAGIL_54077','PAGIL_54973','PAGIL_54997','PAGIL_55056','PAGIL_55091','PAGIL_55108','PAGIL_55187','FITO2PC_43512','FITO2PC_43656','FITO2PC_43657','FITO2PC_43658','FITO2PC_43659','FITO2PC_43660','FITO2PC_43661','FITO2PC_43687','FITO2PC_43688','MTOPRPBLZ_43694','MTOBTTRS_43694','CAHKW_81055','CAHKW_81091','CAHKW_81092','CAHKW_81093','VACRDKW_80174','VACRDKW_81090','VACRDKW_89805','CHQKW_81096','CHQKW_81097','CHQKW_81098','INBKW_81110','MOCKW_81109','LOCAPSH_80288','NETPS_80534','NETPS_80535','BRTWPS_80536','BRTWPS_80537','BRTWPS_80538','CAQKW_80035','CAQKW_80037','CAQKW_80039','CAQKW_80116','CAQKW_81012','CAQKW_81013','CAQKW_81014','CAQKW_81015','CAQKW_81052','CAQKW_81053','CAQKW_81054','CAQKW_81055','CAQKW_81091','CAQKW_81092','CAQKW_81093','CAQKW_81094','CAQKW_81095','CAQKW_89942','CAQSKW_80035','CAQSKW_80037','CAQSKW_80039','CAQSKW_80116','CAQSKW_81012','CAQSKW_81013','CAQSKW_81014','CAQSKW_81015','CAQSKW_81052','CAQSKW_81053','CAQSKW_81054','CAQSKW_81055','CAQSKW_81091','CAQSKW_81092','CAQSKW_81093','CAQSKW_81094','CAQSKW_81095','CAQSKW_89942','CAVKW_80035','CAVKW_80037','CAVKW_80039','CAVKW_80116','CAVKW_81012','CAVKW_81013','CAVKW_81014','CAVKW_81015','CAVKW_81052','CAVKW_81053','CAVKW_81054','CAVKW_81055','CAVKW_81091','CAVKW_81092','CAVKW_81093','CAVKW_81094','CAVKW_81095','CAVKW_89942','VACCKW_80174','VACCKW_81050','VACCKW_81051','VACCKW_81090','VACCKW_89805','VAQKW_80174','VAQKW_80175','VAQKW_81050','VAQKW_81051','VAQKW_81090','VAQKW_89805','CHCKW_81056','CHCKW_81057','CHCKW_81058','CHCKW_81059','CHCKW_81096','CHCKW_81097','CHCKW_81098','TUCBSH_80004','TUCBSH_80005','TUCBSH_80109','TUCBSH_80110','TUCBSH_80212','TUCBSH_80213','TUCBSH_80214','TUCBSH_80236','TUCBSH_80237','TUCBSH_80239','TUCBSH_80240','TUCSH_80101','TUCSH_80104','TUCSH_80106','TUCSH_80108','TUCSH_80142','TUCSH_80165','TUCSH_80166','TUCSH_80206','TUCSH_80207','TUCSH_80208','TUCSH_80209','TUCSH_80210','TUCSH_80211','TUCSH_80227','TUCSH_80230','TUCSH_80231','TUCSH_80238','TUCSH_80241','TUCSH_80242','TUCSH_80281','TUCSH_80282','TUCSH_80284','TUCSH_80285','TUCSH_80286','TUCSH_80287','TUCSH_80289','TUCSH_89926','TUCPSH_80217','TUCPSH_80218','TUCPSH_80284','TUCPSH_80285','DUTSH_80100','DUTSH_80101','DUTSH_80102','DUTSH_80103','DUTSH_80149','DUTSH_80163','DUTSH_80167','DUTSH_80169','DUTSH_80204','DUTSH_80205','DUTSH_80210','DUTSH_80211','DUTSH_80228','DUTSH_80229','DUTSH_80236','DUTSH_80237','DUTSH_80239','DUTSH_80240','DUTSH_80250','DUTSH_80251','DUTSH_80278','DUTSH_80279','DUTSH_80281','DUTSH_80283','DUTSH_80289','DUTSH_80290','KITWSH_80226','KITWSH_89215','KITWSH_89220','KITWSH_89222','KITWSH_89809','KITWSH_89810','KITWSH_89947','KITWSH_89948','KITWSH_89949','MACPS_80500','MACPS_80501','MACPS_80502','MACPS_80503','MACPS_80504','MACPS_80516','MACPS_80531','MACPS_80532','MACPS_80533','CLTS_80510','CLTS_80511','CLTS_80539','CLTS_80540','CLTS_89856','GLYLEWC_43587','GLYLEWC_43591','GLYLEWC_43592','GLYLEWC_43627','GLYLEWC_43654','GLYLEWC_43696','FILE2PC_43587','FILE2PC_43591','FILE2PC_43592','FILE2PC_43627','FILE2PC_43654','FILE2PC_43695','FILE2PC_43696','FILE2PC_43697','FILE2PC_43698','FILE2PC_43699','FILE2PC_43700','FILE2PC_43701','FILEBLZ_43654','FILEBLZ_43699','FILEBLZ_43700','FILEBLZ_55010','FILEBLZ_55076','FILEBLZ_55407','DUCWSLTRS_61849','DUCWSLTRS_62054','DUCWSLTRS_62082','DUCWSLTRS_62087','DCWNCSLTRS_61817','DCWNCSLTRS_62011','DCWNCSLTRS_62012','CATWSLTRS_61849','CATWSLTRS_62054','CATWSLTRS_62082','CATWSLTRS_62087','LISLBLZ_1449','LISLBLZ_27779','LISLBLZ_41933','LISLBLZ_41937','LISLBLZ_42031','LISLBLZ_43317','LISLBLZ_43324','LISLBLZ_43382','LISLBLZ_43389','LISLBLZ_43411','LISLBLZ_43412','LISLBLZ_43507','LISLBLZ_43589','LISLBLZ_43590','LISLBLZ_43600','LISLBLZ_43601','LISLBLZ_43632','LISLBLZ_50942','LISLBLZ_51415','LISLBLZ_52060','LISLBLZ_52462','LISLBLZ_52885','LISLBLZ_52886','LISLBLZ_52994','LISLBLZ_53882','LISLBLZ_54033','LISLBLZ_54172','LISLBLZ_54189','LISLBLZ_54208','LISLBLZ_54358','LISLBLZ_54376','LISLBLZ_54387','LISLBLZ_54453','LISLBLZ_54461','LISLBLZ_54512','LISLBLZ_54514','LISLBLZ_54567','LISLBLZ_54570','LISLBLZ_54639','LISLBLZ_54644','LISLBLZ_54923','LISLBLZ_54962','LISLBLZ_54963','LISLBLZ_55013','FISL2PC_41053','FISL2PC_41458','FISL2PC_41758','FISL2PC_41937','FISL2PC_43292','FISL2PC_43309','FISL2PC_43317','FISL2PC_43382','FISL2PC_43411','FISL2PC_43414','FISL2PC_43544','FISL2PC_43589','FISL2PC_43590','FISL2PC_43600','FISL2PC_43601','FISL2PC_43632','FISLBLZ_27779','FISLBLZ_43632','FISLBLZ_50942','FISLBLZ_51415','FISLBLZ_54033','FISLBLZ_54189','FISLBLZ_54208','FISLBLZ_54453','FISLBLZ_54567','FISLBLZ_54639','LI2PC_43512','LI2PC_43542','LI2PC_43544','LI2PC_43546','LI2PC_43583','LI2PC_43584','LI2PC_43587','LI2PC_43588','LI2PC_43589','LI2PC_43590','LI2PC_43591','LI2PC_43592','LI2PC_43627','LI2PC_43628','LI2PC_43629','LI2PC_43630','LI2PC_43632','LI2PC_43633','LI2PC_43650','LI2PC_43651','LI2PC_43652','LI2PC_43653','LI2PC_43654','LI2PC_43655','LI2PC_43656','LI2PC_43657','LI2PC_43658','LI2PC_43659','LI2PC_43660','LI2PC_43661','LI2PC_43662','LI2PC_43663','LI2PC_43687','LI2PC_43688','LILE2PC_17642','LILE2PC_17655','LILE2PC_40834','LILE2PC_41053','LILE2PC_41410','LILE2PC_41622','LILE2PC_41711','LILE2PC_41758','LILE2PC_41901','LILE2PC_41933','LILE2PC_41937','LILE2PC_41943','LILE2PC_41944','LILE2PC_42031','LILE2PC_42042','LILE2PC_42043','LILE2PC_42044','LILE2PC_42045','LILE2PC_42053','LILE2PC_42057','LILE2PC_42067','LILE2PC_42069','LILE2PC_43005','LILE2PC_43068','LILE2PC_43090','LILE2PC_43091','LILE2PC_43095','LILE2PC_43101','LILE2PC_43111','LILE2PC_43132','LILE2PC_43204','LILE2PC_43206','LILE2PC_43283','LILE2PC_43305','LILE2PC_43308','LILE2PC_43335','LILE2PC_43336','LILE2PC_43375','LILE2PC_43382','LILE2PC_43391','LILE2PC_43409','LILE2PC_43413','LILE2PC_43544','LILE2PC_43587','LILE2PC_43591','LILE2PC_43592','LILE2PC_43597','LILE2PC_43598','LILE2PC_43599','LILE2PC_43627','LILE2PC_43654','LILE2PC_43695','LILE2PC_43696','LILE2PC_43697','LILE2PC_43698','LILE2PC_43699','LILE2PC_43700','LILE2PC_43701','LILE2PC_54567','LISL2PC_17642','LISL2PC_41053','LISL2PC_41410','LISL2PC_41417','LISL2PC_41458','LISL2PC_41622','LISL2PC_41758','LISL2PC_41933','LISL2PC_41937','LISL2PC_42031','LISL2PC_42042','LISL2PC_43068','LISL2PC_43095','LISL2PC_43111','LISL2PC_43237','LISL2PC_43245','LISL2PC_43292','LISL2PC_43309','LISL2PC_43317','LISL2PC_43324','LISL2PC_43361','LISL2PC_43375','LISL2PC_43382','LISL2PC_43389','LISL2PC_43394','LISL2PC_43398','LISL2PC_43411','LISL2PC_43412','LISL2PC_43413','LISL2PC_43414','LISL2PC_43507','LISL2PC_43520','LISL2PC_43521','LISL2PC_43532','LISL2PC_43544','LISL2PC_43589','LISL2PC_43590','LISL2PC_43596','LISL2PC_43600','LISL2PC_43601','LISL2PC_43602','LISL2PC_43603','LISL2PC_43632','LISL2PC_54567','SSMMFIBLZ_41803','SSMMFIBLZ_43561','SSMMFIPTRS_41803','SSMMFIPTRS_43561','SSMMFIPTRS_43653','SSMMFIWC_41803','FITPTRS_43633','FITPTRS_43653','FITPTRS_62059','FITPTRS_62060','FITPTRS_62061','FITPTRS_62062','FITPTRS_62063','FITPTRS_62100','FITPTRS_62102','AMCOAT_55180','OLCOAT_55182','SACOAT_55181','SACOAT_55464','GRCOAT_54473','GRCOAT_54522','GRCOAT_55030','GRCOAT_55045','GRCOAT_55058','GRCOAT_55075','GRCOAT_55152','GRCOAT_55413','GRCOAT_55419','GRCOAT_55461','GRCOAT_55467','RADR_55466','RADR_80280','MIBLZ_54522','MIBLZ_55011','MIBLZ_55031','MIBLZ_55032','MIBLZ_55045','MIBLZ_55104','MIBLZ_55105','MIBLZ_55413','MIBLZ_55415','MIBLZ_55451','MIBLZ_55461','MIBLZ_55462','MIBLZ_55467','MYBLZ_43633','MYBLZ_55011','MYBLZ_55026','MYBLZ_55100','MYBLZ_55414','MYBLZ_55462','MYBLZ_55466','HACRDKW_81108','JEKW_81107','ZOKW_81111','ASBLZ_55205','ASBLZ_55416','ASBLZ_55470','MTOEMSK_55045','MTOEMSK_55415','MTOEMSK_55419','MTOEMSK_55461','MTOEMSK_55467','NISK_55205','NISK_55416','NISK_55470','NISK_70029','MTOBPTRS_54623','MTOBPTRS_55414','MTOBPTRS_00011','MTOBPTRS_00014','MTOBPTRS_00010','MTOBPTRS_00016','HASH_80152','HASH_80153','HASH_80159','HASH_80201','HASH_80202','HASH_80236','HASH_80245','HASH_80246','HASH_80248','HASH_80270','HASH_80273','HASH_80276','HASH_80277','LOSH_70025','LOSH_70026','LOSH_80236','LOSH_80238','LOSH_80272','ALSH_80280','ALSH_80293','ROGIL_55090','ROGIL_55202','ROGIL_55203','LCVKW_81103','LCVKW_81104','MICKW_81071','MICKW_81072','MICKW_81101','MICKW_81102','CAGIL_54385','CAGIL_54794','CAGIL_54915','CAGIL_54964','CAGIL_55004','CAGIL_55011','CAGIL_55012','CAGIL_55015','CAGIL_55051','CAGIL_55057','CAGIL_55077','CAGIL_55112','CAGIL_55402','CAGIL_55404','CAGIL_55407','CAGIL_55409','CAGIL_55411','CAGIL_55412','CAGIL_55460','CAGIL_55463','CAGIL_55464','NBTWA5UL_51600','NBTWA5UL_55223','NBTWA5UL_89162','NBLIA5UL_30005','NBLIA5UL_30006','NBLIA5UL_30007','MUMACNS_53866','CLBLZ_54385','CLBLZ_54915','CLBLZ_54966','CLBLZ_55009','CLBLZ_55012','CLBLZ_55033','CLBLZ_55035','CLBLZ_55036','CLBLZ_55051','CLBLZ_55115','CLBLZ_55116','CLBLZ_55117','CLBLZ_55118','CLBLZ_55471','CLBLZ_55472','MTOBABLZ_55453','MTOBABLZ_55464','GLYWC_43512','GLYWC_43542','GLYWC_43544','GLYWC_43546','GLYWC_43581','GLYWC_43582','GLYWC_43583','GLYWC_43584','GLYWC_43587','GLYWC_43589','GLYWC_43590','GLYWC_43591','GLYWC_43592','GLYWC_43627','GLYWC_43628','GLYWC_43629','GLYWC_43630','GLYWC_43632','GLYWC_43633','GLYWC_43650','GLYWC_43651','GLYWC_43652','GLYWC_43653','GLYWC_43654','GLYWC_43656','GLYWC_43657','GLYWC_43658','GLYWC_43659','GLYWC_43660','GLYWC_43661','GLYWC_43662','GLYWC_43663','GLYWC_43687','GLYWC_43688','GLYWC_52913','GLYWC_52914','GLYWC_52937','GLYWC_55012','GLYWC_55051','GLYWC_55061','GLYWC_55062','GLYWC_55063','GLYWC_55076','GLYWC_55080','GLYWC_55081','GLYWC_55107','GLYWC_55405','GLYWC_55408','GLYWC_55411','GLYWC_62059','GLYWC_62060','GLYWC_62061','GLYWC_62062','GLYWC_62063','GLYWC_62100','GLYWC_62102','GLYWC_62104','GLYWC_62105','ADFCKW_81113','ADFCKW_81114','KIACKW_81115','KIACKW_81116','MOBLZ_54505','MOBLZ_54623','MOBLZ_55028','MOBLZ_55046','MOBLZ_55101','MOBLZ_55418','MOBLZ_55450','MOBLZ_55453','MTOWFSCTRS_54505','MTOWFSCTRS_00015','MTOWFSCTRS_00012','FI2PC_43512','FI2PC_43542','FI2PC_43544','FI2PC_43583','FI2PC_43584','FI2PC_43587','FI2PC_43589','FI2PC_43590','FI2PC_43591','FI2PC_43592','FI2PC_43627','FI2PC_43628','FI2PC_43629','FI2PC_43630','FI2PC_43632','FI2PC_43633','FI2PC_43650','FI2PC_43651','FI2PC_43652','FI2PC_43653','FI2PC_43654','FI2PC_43656','FI2PC_43657','FI2PC_43658','FI2PC_43659','FI2PC_43660','FI2PC_43661','FI2PC_43662','FI2PC_43663','FI2PC_43687','FI2PC_43688','MTOFPTRS_00013','BRBLZNS_24758','BRBLZNS_52715','BRBLZNS_52716','BRBLZNS_52993','BRBLZNS_55121','BRBLZNS_55477','TARBLZNS_52993','TARBLZNS_55478','MOMAC_54781','MOMAC_55039','MOMAC_55185','MOMAC_55186','LIBLZ_43632','LIBLZ_43650','LIBLZ_43651','LIBLZ_43652','LIBLZ_43653','LIBLZ_43654','LIBLZ_43656','LIBLZ_54915','LIBLZ_55010','LIBLZ_55011','LIBLZ_55012','LIBLZ_55013','LIBLZ_55014','LIBLZ_55015','LIBLZ_55017','LIBLZ_55029','LIBLZ_55051','LIBLZ_55052','LIBLZ_55053','LIBLZ_55106','LIBLZ_55107','LIBLZ_55108','LIBLZ_55109','LIBLZ_55110','LIBLZ_55111','LIBLZ_55112','LIBLZ_55119','LIBLZ_55120','LIBLZ_55400','LIBLZ_55401','LIBLZ_55402','LIBLZ_55404','LIBLZ_55405','LIBLZ_55406','LIBLZ_55408','LIBLZ_55409','LIBLZ_55410','LIBLZ_55411','LIBLZ_55421','LIBLZ_55453','LIBLZ_55457','LIBLZ_55458','LIBLZ_55459','LIBLZ_55460','LIBLZ_55463','LIBLZ_55464','LIBLZ_55465','LIBLZ_55474','LIBLZ_55475','LIBLZ_55476','FIPBLZ_43650','FIPBLZ_43651','FIPBLZ_43652','FIPBLZ_43654','FIPBLZ_43656','FIPBLZ_55010','FIPBLZ_55011','FIPBLZ_55016','FIPBLZ_55017','FIPBLZ_55052','FIPBLZ_55053','FIPBLZ_55106','FIPBLZ_55107','FIPBLZ_55108','FIPBLZ_55109','FIPBLZ_55112','FIPBLZ_55400','FIPBLZ_55401','FIPBLZ_55402','FIPBLZ_55406','FIPBLZ_55410','FIPBLZ_55421','FIPBLZ_55457','FIPBLZ_55458','FIPBLZ_55459','FIPBLZ_55460','FIPBLZ_55463','FIPBLZ_55464','FIPBLZ_55465','LILEBLZ_1449','LILEBLZ_41933','LILEBLZ_43005','LILEBLZ_43095','LILEBLZ_43204','LILEBLZ_43283','LILEBLZ_43305','LILEBLZ_43375','LILEBLZ_43382','LILEBLZ_43599','LILEBLZ_43654','LILEBLZ_43699','LILEBLZ_43700','LILEBLZ_50942','LILEBLZ_50946','LILEBLZ_51017','LILEBLZ_52060','LILEBLZ_52462','LILEBLZ_52553','LILEBLZ_52886','LILEBLZ_52994','LILEBLZ_53475','LILEBLZ_54025','LILEBLZ_54063','LILEBLZ_54194','LILEBLZ_54249','LILEBLZ_54250','LILEBLZ_54258','LILEBLZ_54387','LILEBLZ_54440','LILEBLZ_54453','LILEBLZ_54567','LILEBLZ_54644','LILEBLZ_54923','LILEBLZ_54926','LILEBLZ_55010','LILEBLZ_55076','LILEBLZ_55407','SSMMLIDBLZ_5281','SSMMLIDBLZ_41662','TARWCNS_24758','TARWCNS_52715','TARWCNS_52716','TARWCNS_52993','TARWCNS_55121','TARWCNS_55477','TARWCNS_55478']

async function getProduct() {
    // Get all files in the product folder
    const files = await fs.promises.readdir(productFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} product CSV files in ${productFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const productFileName = path.join(productFolderPath, csvFile);
        console.log(`Processing product file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(productFileName)
                .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
                .on('error', error => {
                    console.error(`Error processing product file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', async row => {
                    stream.pause();
                    
                    try {
                        const supplierCode = parseInt(row['preferred supplier code']);
                        console.log(supplierCode);
                        
                        if (manufacturers[supplierCode] == undefined) {
                            let contacts = [];
                            if (suppliers[supplierCode]['contact email'] != '' || suppliers[supplierCode]['contact telephone'] != '') {
                                contacts = [{
                                    "forename": suppliers[supplierCode]['short name'],
                                    "surname": "",
                                    "email": suppliers[supplierCode]['contact email'],
                                    "phone": suppliers[supplierCode]['contact telephone'],
                                    "tags": [],
                                    "name": {
                                        "forename": suppliers[supplierCode]['short name'],
                                        "surname": ""
                                    }
                                }];
                            }
                            
                            await common.requester('post', `https://api.stok.ly/v0/manufacturers`, {
                                "name": suppliers[supplierCode].name + ' - ' + suppliers[supplierCode].code,
                                "accountReference": suppliers[supplierCode]['default nominal account number'],
                                "vatNumber": suppliers[supplierCode]['tax registration number'] != '' ? {
                                    "value": suppliers[supplierCode]['tax registration number'],
                                    "country": suppliers[supplierCode]['country code']
                                } : undefined,
                                "currency": suppliers[supplierCode]['contact email'].currency,
                                contacts: contacts,
                                "addresses": [
                                    {
                                        "line1": suppliers[supplierCode]['address line1'].length > 2 ? suppliers[supplierCode]['address line1'] : 'undefined',
                                        "line2": suppliers[supplierCode]['address line2'] > 2 ? suppliers[supplierCode]['address line2'] : '',
                                        "city": suppliers[supplierCode]['city'] > 2 ? suppliers[supplierCode]['city'] : 'undefined',
                                        "region": suppliers[supplierCode]['county'] > 2 ? suppliers[supplierCode]['county'] : '',
                                        "country": suppliers[supplierCode]['country code'],
                                        "postcode": "undefined"
                                    }
                                ]
                            }).then(r => {
                                manufacturers[supplierCode] = r.data.data.id;
                            });
                        }
                        
                        supplierLookup[row['product code']] = manufacturers[parseInt(row['preferred supplier code'])];
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    console.log('Finished processing all product files');
}

async function getAllManufacturers(){
    await common.loopThrough('Getting Manufacturers', `https://api.stok.ly/v0/manufacturers`, 'size=1000', '[status]=={active}', async (m)=>{
        try{
            manufacturers[parseInt(m.name.split('-')[1].toLowerCase().trim())] = m.manufacturerId    
        } catch {}
    })
};

async function getCustomersSheet(){
    return new Promise((res,rej)=>{
        const stream = fs.createReadStream(supplierSheet)
        .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
        .on('error', error => console.error(error))
        .on('data',  row => {
            stream.pause()

                let customerCode
                isNaN(row['code']) ? customerCode = row['code'] : customerCode = parseInt(row['code'])
                suppliers[customerCode] = row

            stream.resume()
        })
        .on('end', async () => {
            res()
        })
    })
}

async function getBomDetails() {
    // Get all files in the BOM details folder
    const files = await fs.promises.readdir(bomDetailsFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} BOM details CSV files in ${bomDetailsFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const BOMDetailsName = path.join(bomDetailsFolderPath, csvFile);
        console.log(`Processing BOM details file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(BOMDetailsName)
                .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
                .on('error', error => {
                    console.error(`Error processing BOM details file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', row => {
                    stream.pause();
                    
                    try {
                        if (BomDetailsLookup[row['rm code']] == undefined) {
                            BomDetailsLookup[row['rm code']] = {};
                        }
                        BomDetailsLookup[row['rm code']].type = row['rm type description'];
                        if (!itemTypesToMake.includes(row['rm type description'])) {
                            itemTypesToMake.push(row['rm type description']);
                        }
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    console.log('Finished processing all BOM details files');
    console.log(`Total unique RM codes processed: ${Object.keys(BomDetailsLookup).length}`);
    console.log(`Item types to make: ${itemTypesToMake.join(', ')}`);
}


async function getBomNumbers() {
    // Get all files in the BOM numbers folder
    const files = await fs.promises.readdir(bomFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} BOM number CSV files in ${bomFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const BOMFileName = path.join(bomFolderPath, csvFile);
        console.log(`Processing BOM number file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(BOMFileName)
                .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
                .on('error', error => {
                    console.error(`Error processing BOM number file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', row => {
                    stream.pause();
                    
                    try {
                        if (BOMNumbers[row['product code']] == undefined) {
                            BOMNumbers[row['product code']] = {};
                        }
                        
                        if (BOMNumbers[row['product code']][row['product colour code']] == undefined) {
                            BOMNumbers[row['product code']][row['product colour code']] = parseInt(row['bom code']);
                        }
                        
                        if (BOMNumbers[row['product code']][row['product colour code']] < parseInt(row['bom code'])) {
                            BOMNumbers[row['product code']][row['product colour code']] = parseInt(row['bom code']);
                        }
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    console.log('Finished processing all BOM number files');
    
    // Calculate some statistics for logging
    let totalProducts = 0;
    let totalProductColorCombinations = 0;
    
    for (const productCode in BOMNumbers) {
        totalProducts++;
        totalProductColorCombinations += Object.keys(BOMNumbers[productCode]).length;
    }
    
}

async function getBomRawCosts() {
    // Get all files in the BOM raw costs folder
    const files = await fs.promises.readdir(bomRawCostsFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} BOM raw costs CSV files in ${bomRawCostsFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const BOMRawCostsCSV = path.join(bomRawCostsFolderPath, csvFile);
        console.log(`Processing BOM raw costs file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(BOMRawCostsCSV)
                .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
                .on('error', error => {
                    console.error(`Error processing BOM raw costs file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', row => {
                    stream.pause();
                    
                    try {
                        BOMRawCosts[`${row['product/rm code']}_${row['colour code']}`] = row['product purchase price'];
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    console.log('Finished processing all BOM raw costs files');
    console.log(`Total unique product/color combinations with costs: ${Object.keys(BOMRawCosts).length}`);
}

async function getBomCosts() {
    // Get all files in the BOM costs folder
    const files = await fs.promises.readdir(bomCostsFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} BOM costs CSV files in ${bomCostsFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const BOMCosts = path.join(bomCostsFolderPath, csvFile);
        console.log(`Processing BOM costs file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(BOMCosts)
                .pipe(csv.parse({headers: headers => headers.map(h => h.toLowerCase().trim())}))
                .on('error', error => {
                    console.error(`Error processing BOM costs file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', async row => {
                    stream.pause();
                    
                    try {
                        if (additionalCosts[`${row['product/rm code']}_${row['colour code']}`] == undefined) {
                            additionalCosts[`${row['product/rm code']}_${row['colour code']}`] = [];
                        }
                        
                        additionalCosts[`${row['product/rm code']}_${row['colour code']}`].push({
                            name: row['additional cost name'],
                            amount: row['value'],
                            type: row.type
                        });
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    console.log('Finished processing all BOM costs files');
    
    // Calculate some statistics for logging
    let totalProductColorCombinations = Object.keys(additionalCosts).length;
    let totalAdditionalCosts = 0;
    
    for (const key in additionalCosts) {
        totalAdditionalCosts += additionalCosts[key].length;
    }
    
    console.log(`Total unique product/color combinations with additional costs: ${totalProductColorCombinations}`);
    console.log(`Total additional cost entries: ${totalAdditionalCosts}`);
}

async function getBoms() {
    // Track products without specific color code across all files
    let all = {};
    
    // Get all files in the BOM folder
    const files = await fs.promises.readdir(bomFolderPath);
    const csvFiles = files.filter(file => file.toLowerCase().endsWith('.csv'));
    
    console.log(`Found ${csvFiles.length} BOM CSV files in ${bomFolderPath}`);
    
    // Process each file sequentially
    for (const csvFile of csvFiles) {
        const BOMFileName = path.join(bomFolderPath, csvFile);
        console.log(`Processing BOM file: ${csvFile}...`);
        
        await new Promise((res, rej) => {
            const stream = fs.createReadStream(BOMFileName)
                .pipe(csv.parse({
                    headers: headers => headers.map(h => h.toLowerCase().trim())
                }))
                .on('error', error => {
                    console.error(`Error processing BOM file ${csvFile}:`, error);
                    rej(error);
                })
                .on('data', async row => {
                    stream.pause();
                    
                    try {
                        if (BOMNumbers[row['product code']] && 
                            BOMNumbers[row['product code']][row['product colour code']] == row['bom code']) {
                            
                            if (BOMS[`${row['product code']}`] == undefined) {
                                BOMS[`${row['product code']}`] = {};
                            }
                            
                            let paddedColourCode = (() => {
                                if (!isNaN(row['rm colour code'])) {
                                    return row['rm colour code'].padStart(4, '0');
                                }
                                return row['rm colour code'];
                            })();
                            
                            let product = {
                                sku: [row['rm code'], paddedColourCode, row['rm size code']].join('-'),
                                label: row['bom line name'],
                                amount: parseFloat(row['rm usage']),
                                type: BomDetailsLookup[row['rm code']]?.type || 'unknown'
                            };
                            
                            if (row['product colour code'].trim() != '') {
                                if (BOMS[`${row['product code']}`][row['product colour code']] == undefined) {
                                    BOMS[`${row['product code']}`][row['product colour code']] = [];
                                }
                                
                                let alreadyIn = false;
                                for (const item in BOMS[`${row['product code']}`][row['product colour code']]) {
                                    if (BOMS[`${row['product code']}`][row['product colour code']][item].sku == [row['rm code'], paddedColourCode, row['rm size code']].join('-')) {
                                        BOMS[`${row['product code']}`][row['product colour code']][item].amount += parseFloat(row['rm usage']);
                                        alreadyIn = true;
                                    }
                                }
                                
                                if (!alreadyIn) {
                                    BOMS[`${row['product code']}`][row['product colour code']].push(product);
                                }
                            } else {
                                if (all[row['product code']] == undefined) {
                                    all[row['product code']] = [];
                                }
                                
                                // Check if the item is already in the 'all' array
                                let alreadyInAll = false;
                                for (const item in all[row['product code']]) {
                                    if (all[row['product code']][item].sku == product.sku) {
                                        all[row['product code']][item].amount += product.amount;
                                        alreadyInAll = true;
                                        break;
                                    }
                                }
                                
                                if (!alreadyInAll) {
                                    all[row['product code']].push(product);
                                }
                            }
                        }
                    } catch (error) {
                        console.error(`Error processing row in ${csvFile}:`, error);
                    }
                    
                    stream.resume();
                })
                .on('end', async () => {
                    console.log(`Finished processing ${csvFile}`);
                    res();
                });
        });
    }
    
    // Process the combined 'all' items
    await common.sleep(200);
    console.log('Processing color-agnostic BOM items...');
    
    for (const item of Object.keys(all)) {
        if (BOMS[item]) {
            for (const parent of Object.keys(BOMS[item])) {
                BOMS[item][parent].push(...all[item]);
            }
        }
    }
    
    console.log('Finished processing all BOM files');
    
    // Calculate some statistics for logging
    let totalProducts = Object.keys(BOMS).length;
    let totalProductColorCombinations = 0;
    let totalBOMItems = 0;
    
    for (const productCode in BOMS) {
        totalProductColorCombinations += Object.keys(BOMS[productCode]).length;
        
        for (const colorCode in BOMS[productCode]) {
            totalBOMItems += BOMS[productCode][colorCode].length;
        }
    }
    
    console.log(`Total products with BOMs: ${totalProducts}`);
    console.log(`Total product-color combinations: ${totalProductColorCombinations}`);
    console.log(`Total BOM items: ${totalBOMItems}`);
}


async function getAllVariables(){
    await common.loopThrough('Getting Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]=*{2}))%26%26([status]!={1})', async (item)=>{
        if (!tempList.includes(item.sku)){return}
        itemIDLookup[item.sku.toLowerCase().trim()] = item.itemId
        itemList[item.sku] = []
        await common.loopThrough('', `https://api.stok.ly/v0/items/${item.itemId}/children`, 'size=1000', '([status]!={1})', async (childItem)=>{
            itemListLookup[childItem.itemId] = childItem.sku
            itemList[item.sku].push(childItem.itemId)
        })
    })
};

async function getAllCurrentBOMS(){
    await common.loopThrough('Getting BOM Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]=*{3}))%26%26([status]!={1})', async (item)=>{
        allCurrentBoms.push(item.itemId)
    })
};

async function getAllSimples(){
    await common.loopThrough('Getting Simple Items', `https://api.stok.ly/v0/items`, 'size=1000&sortDirection=ASC&sortField=name', '(([format]=*{0}))%26%26([status]!={1})%26%26[tags]::{RM}', async (item)=>{
        simpleProducts[item.name.toLowerCase().trim()] = item.itemId
    })
};


async function getAllTypes(){
    await common.loopThrough('Getting Types', `https://api.stok.ly/v0/item-types`, 'size=1000&sortDirection=ASC&sortField=name', '[status]!={1}', async (type)=>{
        itemTypeLookup[type.name.toLowerCase()] = type.itemTypeId
    })
};

async function makeItemTypes(){
    for (const type of itemTypesToMake){
        if (itemTypeLookup[type.toLowerCase()] == undefined){
            await common.requester('post', 'https://api.stok.ly/v0/item-types', {"name": type}).then(r=>{
                itemTypeLookup[type.toLowerCase()] = r.data.data.id
            })
        }
    }
    
}


async function makeBoms(){

    let promiseArr = []
    for (const BOM of Object.keys(BOMS)){
        promiseArr.push(makeSingleBOM(BOM))
        if (promiseArr.length >= 7){
            await Promise.all(promiseArr)
            promiseArr = []
        }
    }
    await Promise.all(promiseArr)

    async function makeSingleBOM(BOM){
        for (const fabric of Object.keys(BOMS[BOM])){
            let templateList = []
            let billOfMaterialsTemplate = []
            try{
                let manufacturingCostsParent = [{
                    "manufacturerId": supplierLookup[BOM],
                    "taxClassId": defaultTaxClass,
                    "name": `Cost of Item => ${fabric}`,
                    "cost": BOMRawCosts[`${BOM}_${fabric}`],
                    "tax": 0,
                    "rate": 0,
                    automaticallyIncludeOnManufacturingRuns: true
                }]
    
                for (const additional of additionalCosts[`${BOM}_${fabric}`]){
                    manufacturingCostsParent.push({
                        "manufacturerId": supplierLookup[BOM],
                        "taxClassId": defaultTaxClass,
                        "name": `${additional.name} => ${fabric}`,
                        "cost": additional.type.toLowerCase() == 'percentage' ? BOMRawCosts[`${BOM}_${fabric}`] * (additional.amount/100) : additional.amount,
                        "tax": 0,
                        "rate": 0,
                        automaticallyIncludeOnManufacturingRuns: false
                    })
                }
                for (const item of itemList[`${BOM}_${fabric}`]){

                    let manufacturingCostsChild = [{
                        "manufacturerId": supplierLookup[BOM],
                        "taxClassId": defaultTaxClass,
                        "name": `Cost of Item => ${itemListLookup[item]}`,
                        "cost": BOMRawCosts[`${BOM}_${fabric}`],
                        "tax": 0,
                        "rate": 0,
                        automaticallyIncludeOnManufacturingRuns: true
                    }]
        
                    for (const additional of additionalCosts[`${BOM}_${fabric}`]){
                        manufacturingCostsChild.push({
                            "manufacturerId": supplierLookup[BOM],
                            "taxClassId": defaultTaxClass,
                            "name": `${additional.name} => ${itemListLookup[item]}`,
                            "cost": additional.type.toLowerCase() == 'percentage' ? BOMRawCosts[`${BOM}_${fabric}`] * (additional.amount/100) : additional.amount,
                            "tax": 0,
                            "rate": 0,
                            automaticallyIncludeOnManufacturingRuns: false
                        })
                    }

                    let bomList = []
                    for (const RM of BOMS[BOM][fabric]){
                        if(!templateList.includes(simpleProducts[RM.sku.toLowerCase().trim()])){
                            templateList.push(simpleProducts[RM.sku.toLowerCase().trim()])
                            billOfMaterialsTemplate.push({
                                "label": RM.label,
                                "predicate": `([typeId]=={${itemTypeLookup[RM.type.toLowerCase()]}})`,
                                "sortIndex": billOfMaterialsTemplate.length
                            })
                        }
                        
        
                        bomList.push({
                            "itemId": simpleProducts[RM.sku.toLowerCase().trim()],
                            "billOfMaterialItemId": simpleProducts[RM.sku.toLowerCase().trim()],
                            "label": RM.label,
                            "quantity": parseFloat(RM.amount.toFixed(4))
                        })
        
                    }
        
                    let attempts = 0;
                    const maxRetries = 5;
                    while (attempts < maxRetries) {
                        
                        try {
                            await common.requester('patch', `https://api.stok.ly/v0/items/${item}`, {
                                "acquisition": 2,
                                "billOfMaterials": bomList,
                                manufacturingCosts: manufacturingCostsChild
                            }, 0);
                            break
                        } catch (error) {
                            attempts ++
                            console.log(error)
                        }
                    }
                    console.log(`Done item ${item}`)
                }
                let attempts = 0;
                const maxRetries = 5;
                while (attempts < maxRetries) {
                    try {
                        await common.requester('patch', `https://api.stok.ly/v0/variable-items/${itemIDLookup[`${BOM}_${fabric}`.toLowerCase().trim()]}`, {
                            billOfMaterialsTemplate,
                            manufacturingCosts: manufacturingCostsParent
                        }, 0);
                        break
                    } catch (error) {
                        attempts++;
                        console.log(error)
                    }
                }
            }catch{}
        }
    }

}

async function run(){
    await getAllManufacturers()
    await getCustomersSheet()
    await getProduct()
    await getAllCurrentBOMS()
    await getBomNumbers()
    await getAllSimples()
    await getBomDetails()
    await getAllVariables()
    await getAllTypes()
    await getBoms()
    await makeItemTypes()
    await getBomRawCosts()
    await getBomCosts()

    await makeBoms()

    fs.writeFileSync('./testBOMs.txt', JSON.stringify(BOMS))
    console.log(itemList)
}

module.exports = {
    run
}