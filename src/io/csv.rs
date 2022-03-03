//! Represents different data-import methods

use crate::physical::datatypes::{DataTypeName, Double, Float};
use crate::{error::Error, physical::datatypes::DataValueT};
use csv::Reader;

/// Imports a csv file
/// Needs a list of Options of [Datatypename] and a [csv::Reader] reference
pub fn csv<T>(
    datatypes: &[Option<DataTypeName>],
    csv_reader: &mut Reader<T>,
) -> Result<Vec<VecT>, Error>
where
    T: std::io::Read,
{
    let mut result: Vec<Option<VecT>> = Vec::new();

    datatypes.iter().for_each(|dtype| {
        result.push(dtype.and_then(|dt| {
            Some(match dt {
                DataTypeName::U64 => VecT::VecU64(Vec::new()),
                DataTypeName::Float => VecT::VecFloat(Vec::new()),
                DataTypeName::Double => VecT::VecDouble(Vec::new()),
            })
        }));
    });
    csv_reader.records().for_each(|rec| {
        if let Ok(row) = rec {
            if let Err(Error::RollBack(rollback)) =
                row.iter().enumerate().try_for_each(|(idx, item)| {
                    if let Some(datatype) = datatypes[idx] {
                        match datatype.parse(item) {
                            Ok(val) => {
                                result[idx].as_mut().map(|vect| {
                                    vect.push(&val);
                                    Some(())
                                });
                                Ok(())
                            }
                            Err(e) => {
                                log::error!(
                                    "Ignoring line {:?}, Dataconversion failed: {}",
                                    row,
                                    e
                                );
                                Err(Error::RollBack(idx))
                            }
                        }
                    } else {
                        Ok(())
                    }
                })
            {
                for item in result.iter_mut().take(rollback) {
                    if let Some(vec) = item.as_mut() {
                        vec.pop()
                    }
                }
            }
        }
    });
    Ok(result.into_iter().flatten().collect())
}

/// Enum for vectors of different supported input types
#[derive(Debug)]
pub enum VecT {
    /// Case Vec<u64>
    VecU64(Vec<u64>),
    /// Case Vec<Float>
    VecFloat(Vec<Float>),
    /// Case Vec<Double>
    VecDouble(Vec<Double>),
}

impl VecT {
    /// Removes the last element in the corresponding vector
    pub fn pop(&mut self) {
        match self {
            VecT::VecU64(v) => {
                v.pop();
            }
            VecT::VecFloat(v) => {
                v.pop();
            }
            VecT::VecDouble(v) => {
                v.pop();
            }
        }
    }

    /// Inserts the Value to the corresponding Vector if the datatypes are compatible
    /// Note that it is not checked if the [DataValueT] has the right enum-variant
    pub(crate) fn push(&mut self, value: &DataValueT) {
        match self {
            VecT::VecU64(vec) => vec.push(value.as_u64().unwrap()),
            VecT::VecFloat(vec) => vec.push(value.as_float().unwrap()),
            VecT::VecDouble(vec) => vec.push(value.as_double().unwrap()),
        };
    }

    /// Returns the lengths of the Vector
    pub fn len(&self) -> usize {
        match self {
            VecT::VecU64(vec) => vec.len(),
            VecT::VecFloat(vec) => vec.len(),
            VecT::VecDouble(vec) => vec.len(),
        }
    }

    /// Returns whether the vector is empty, or not
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use csv::ReaderBuilder;
    use test_env_log::test;
    #[test]
    fn csv_empty() {
        let data = "\
city;country;pop
Boston;United States;4628910
";
        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(data.as_bytes());

        let x = csv(&[None, None, None], &mut rdr);
        assert!(x.is_ok());
        assert_eq!(x.unwrap().len(), 0);
    }

    #[test]
    fn csv_with_ignored_and_faulty() {
        let data = "\
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_nc-state-university-wolfline_20150227_1735.gml.50_109_224.apx.adf;6.62178;10.0071;10.0055;10.0056;10.0055;0.07971;0.016235;0.017156;0.003569;0.007411;10.0056;10.0053;10.0059;0.003342;10.0058;10.0453;10.048;10.0485;10.0485;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_northwestpoint_or_2015-12-07.gml.20_11_13.apx.adf;0.014913;0.003004;0.205419;0.006902;0.00902;0.014262;0.002536;0.001563;0.001299;0.002862;0.029051;0.004145;0.004217;0.001763;0.001808;0.001768;0.001758;0.001764;0.001686;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_norwalk_ct_2016-01.gml.80_250_552.apx.adf;10.0161;10.1121;10.0056;10.0057;10.0057;0.524897;0.06249;0.186562;0.008093;0.018879;10.0058;10.0054;10.0057;0.003124;10.005;10.0612;10.1672;10.1724;10.055;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_omit-sainte-julie_20151202_1945.gml.50_152_280.apx.adf;10.1481;10.1087;10.0056;10.0086;10.0057;10.0435;0.021738;0.009776;0.003374;0.010784;10.0052;10.0049;10.0054;0.001995;10.0054;10.0681;10.0683;10.0673;10.1602;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_ouibus_20151219_1959.gml.50_54_164.apx.adf;10.0769;0.118254;10.0055;10.0058;10.0055;10.0482;0.008317;0.003396;0.001978;0.00545;10.0057;10.0049;10.0052;0.001805;10.0053;0.011685;0.017369;0.00991;0.011602;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_palos-verdes-peninsula-transit-authority_20151216_1829.gml.80_192_499.apx.adf;10.0671;10.0101;10.0088;10.0059;10.0057;0.432222;0.037169;0.039913;0.005406;0.01449;10.006;10.0057;10.0061;0.00208;10.0058;10.0497;10.1512;10.0501;10.0503;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_path_20120517_1620.normalized.gml.20_13_15.apx.adf;0.010724;0.002893;0.002974;0.00181;0.001528;0.017388;0.002524;0.001722;0.001531;0.003546;0.002683;0.001733;0.001831;0.001506;0.00175;0.001645;0.001797;0.001644;0.001838;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_razorback-transit_20090814_1752.gml.50_92_188.apx.adf;10.0548;10.0067;10.006;10.0059;10.0059;3.97863;0.013219;0.004925;0.002488;0.008512;10.0053;10.0057;10.0056;0.001794;10.0057;5.275;5.30562;5.32666;5.36696;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_redwood-coast-transit_20151216_1453.gml.20_113_165.apx.adf;1.05099;10.0069;10.0057;10.0059;10.0057;0.105353;0.019818;0.043604;0.00291;0.00941;10.0056;10.0059;10.0058;0.001824;10.0059;10.051;10.0516;10.0506;10.1532;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_rio-vista-delta-breeze_20131212_0124.gml.50_64_107.apx.adf;3.30655;10.1062;10.0057;10.0058;10.0054;0.059267;0.006526;0.002082;0.001434;0.006061;10.0059;10.0057;10.0057;0.002202;10.0059;0.533107;0.512114;0.504704;0.530146;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_rio-vista-delta-breeze_20131212_0124.gml.80_64_127.apx.adf;0.023638;0.063589;10.0053;10.0057;10.0057;0.033001;0.006864;0.004232;0.001762;0.004274;10.0056;10.0059;10.0057;0.002406;10.0061;0.160374;0.17699;0.16749;0.157837;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_rockland_county_ny_2016-01.gml.20_16_32.apx.adf;0.014915;0.005303;10.0056;4.50935;4.51245;0.020771;0.002803;0.001593;0.00157;0.003096;4.44234;0.169853;0.1744;0.001822;0.017962;0.001966;0.002068;0.001614;0.001802;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_roseville_ca_2015-10-21.gml.80_207_449.apx.adf;10.0073;10.0111;10.0047;10.0056;10.0057;0.176259;0.071135;0.086455;0.004996;0.012949;10.0066;10.006;10.0057;0.003661;10.006;10.0539;10.0543;10.0623;10.1607;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_san-benito-county-express_20151216_1918.gml.20_61_91.apx.adf;0.149953;6.24258;10.0056;10.0055;10.0057;0.055438;0.005413;0.006826;0.002244;0.014751;10.0056;10.0056;10.0057;0.003598;10.006;0.311851;0.201615;0.315861;0.212323;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_san_francisco-BART_20160101_v1.gml.20_45_57.apx.adf;0.021236;0.089622;10.0056;10.0055;10.0057;0.02517;0.003558;0.004797;0.001687;0.00523;10.006;10.0052;10.0056;0.001534;10.0058;0.015897;0.016912;0.009917;0.011566;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_sbase_20150429_2048.gml.80_98_171.apx.adf;0.097503;3.19069;10.006;10.0056;10.0054;0.06734;0.010974;0.010531;0.002264;0.006279;10.0056;10.0057;10.0058;0.002072;10.0057;10.0626;10.1635;10.0631;10.0626;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_septa_20151217_1207.gml.80_155_406.apx.adf;10.0613;10.0108;10.0057;10.0056;10.0058;2.79762;0.033997;0.023887;0.004178;0.010344;10.0058;10.0058;10.0056;0.002002;10.0055;10.086;10.0855;10.1863;10.0842;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_sfbay-ferries-archiver_20100127_0320.gml.80_9_26.apx.adf;0.01773;0.004718;0.101675;0.005851;0.008005;0.024586;0.002472;0.001549;0.001506;0.003646;0.020337;0.003705;0.003638;0.00155;0.001779;0.001634;0.001684;0.001577;0.001652;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_siskiyou-transit-and-general-express_20150106_0149.gml.80_117_351.apx.adf;8.30573;10.008;10.0057;10.0059;10.0053;0.362447;0.02003;0.004308;0.002102;0.00981;10.0058;10.0057;10.0056;0.002823;10.0057;10.0512;10.0492;10.051;10.0495;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_societe-nationale-des-chemins-de-fer-tunisiens_20140804_1915.gml.20_18_21.apx.adf;0.010962;0.004396;1.11942;0.034153;0.034928;0.02878;0.002919;0.001854;0.001539;0.004253;0.240356;0.009649;0.010263;0.001554;0.002797;0.002169;0.00204;0.002591;0.002024;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_southern_california_2016-01.gml.20_56_89.apx.adf;0.053515;0.503386;10.0052;10.0058;10.0054;0.062031;0.010076;0.005568;0.002088;0.005919;10.0053;10.0085;10.0057;0.001575;10.0057;0.018474;0.033714;0.017432;0.019478;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_southwest-point_20151216_1538.gml.80_18_34.apx.adf;0.023606;0.00366;2.49098;0.075735;0.076973;0.019658;0.002434;0.00194;0.001381;0.003648;0.351089;0.01924;0.018104;0.001364;0.003316;0.002136;0.001905;0.002366;0.00222;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_s-potniki-promet-doo_20141212_1812.gml.80_262_703.apx.adf;10.0233;10.1124;10.0057;10.0054;10.006;1.53698;0.087591;0.045311;0.003392;0.019912;10.0058;10.0057;10.0058;0.00458;10.0057;10.0926;10.0888;10.0865;10.1868;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_sunshine-bus-company_20151216_1645.gml.80_48_122.apx.adf;0.064779;0.356382;10.0055;10.0058;10.0057;0.154318;0.003121;0.0019;0.001622;0.006076;10.0057;10.0057;10.0057;0.001498;10.0048;0.243419;0.2087;0.206591;0.207281;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_swan-island-tma_20151217_0809.gml.20_15_26.apx.adf;0.011822;0.004855;10.0056;3.88587;3.94695;0.017239;0.002667;0.001567;0.001527;0.00411;3.18397;0.15594;0.165151;0.001495;0.012561;0.001494;0.001994;0.001252;0.001698;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_tehama-archiver_20151217_1411.gml.20_90_138.apx.adf;0.069982;1.58764;10.0063;10.0061;10.0063;0.08939;0.028478;0.046287;0.003482;0.00983;10.0042;10.0056;10.0054;0.002388;10.0053;10.1487;10.0478;10.0475;10.0475;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_tehama_ca_2015-12-30.gml.80_90_212.apx.adf;4.08326;10.007;10.0059;10.0058;10.0057;1.78088;0.023329;0.007046;0.002212;0.006267;10.0059;10.0058;10.0058;0.001669;10.0058;10.0316;10.0316;10.0308;10.0314;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_temiskaming-transit_20140107_0435.gml.20_62_87.apx.adf;0.031109;0.196912;10.0058;10.0058;10.0057;0.065661;0.008762;0.004646;0.002102;0.005643;10.0056;10.0062;10.0056;0.002308;10.0059;0.040388;0.06623;0.061426;0.066713;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_the-shuttle-inc_20141224_0137.gml.20_18_23.apx.adf;0.017495;0.004087;0.012043;0.002622;0.001604;0.050895;0.002773;0.002173;0.001544;0.003478;0.00982;0.002326;0.002135;0.001588;0.002224;0.001732;0.0024;0.0017;0.002348;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_tillamook_or_2016-01.gml.80_181_424.apx.adf;10.0172;10.0099;10.0058;10.0059;10.0058;0.240366;0.038199;0.059925;0.005606;0.014024;10.0054;10.0062;10.0058;0.002881;10.006;10.0922;10.1926;10.0921;10.0916;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_tillamook-or-us.gml.20_182_281.apx.adf;10.0195;10.0104;10.0058;10.0055;10.0058;0.189913;0.0354;0.053298;0.005608;0.012621;10.0059;10.0057;10.006;0.002898;10.0052;10.1559;10.0518;10.0503;10.0517;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_tillamook-or-us.gml.80_182_418.apx.adf;10.0231;10.0112;10.0057;10.0059;10.0055;0.378447;0.06749;0.033747;0.004295;0.013326;10.0055;10.006;10.0051;0.002024;10.0051;10.1417;10.142;10.0438;10.0393;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_topeka-metro_20121026_2234.gml.50_292_589.apx.adf;10.1053;10.0125;10.0057;10.0058;10.0059;0.561904;0.110328;0.102049;0.005834;0.026065;10.0057;10.0061;10.0057;0.003358;10.0057;10.157;10.0573;10.0576;10.0572;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_triangle-transit-express_20150227_1736.gml.20_56_105.apx.adf;0.07355;0.784791;10.0058;10.0054;10.0053;0.183869;0.007916;0.003337;0.002075;0.004422;10.0055;10.0061;10.0057;0.003536;10.0056;0.010146;0.011948;0.015808;0.011557;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_triangle-transit-express_20150227_1736.gml.80_56_154.apx.adf;0.183916;4.77004;10.0057;10.005;10.0058;0.190881;0.010507;0.005632;0.001867;0.006332;10.0058;10.0054;10.0054;0.003726;10.0057;0.107776;0.129203;0.131607;0.130657;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_tri-rail_20090129_0554.gml.20_18_20.apx.adf;0.024032;0.00396;0.720665;0.019171;0.014655;0.019448;0.002766;0.001828;0.001521;0.003657;0.091801;0.003589;0.006906;0.001801;0.00221;0.001882;0.002193;0.001911;0.002163;3;1200;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_tri-rail_20090129_0554.gml.50_18_29.apx.adf;0.017449;0.007189;10.0054;10.0059;10.0055;0.020392;0.002637;0.001316;0.001503;0.003564;10.0062;1.54404;1.54779;0.00153;0.09275;0.001188;0.002146;0.002074;0.a002134;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_tursib_20110626_1306.gml.50_212_426.apx.adf;10.1086;10.0105;10.0057;10.0056;10.0058;0.231068;0.040038;0.018688;0.005047;0.016373;10.0055;10.0056;10.106;0.002093;10.0057;10.0596;10.1594;10.058;10.1599;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_umpquatransit_or_2015-12-03.gml.50_190_349.apx.adf;10.0553;10.1105;10.0061;10.006;10.0059;0.393763;0.02991;0.022196;0.003573;0.012327;10.0053;10.0057;10.0057;0.003938;10.0057;10.0547;10.0547;10.0552;10.1552;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_university-of-nairobi-c4dlab_20141208_0940.normalized.gml.80_274_596.apx.adf;10.0946;10.0118;10.0055;10.005;10.0056;0.266306;0.092175;0.210805;0.004646;0.021937;10.0061;10.006;10.005;0.003592;10.0055;10.0538;10.1482;10.0539;10.0473;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_valleyretriever_or_2015-12-01.gml.20_14_20.apx.adf;0.010828;0.004252;10.0055;1.11843;1.13481;0.014143;0.00254;0.001554;0.001503;0.003238;1.54789;0.062909;0.063618;0.001724;0.009817;0.002749;0.001732;0.002736;0.001796;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_vitoria-gasteiz_2016-01.gml.20_279_455.apx.adf;10.0557;10.1114;10.0058;10.0053;10.0061;0.356287;0.0658;0.021588;0.004151;0.022403;10.0056;10.0057;10.0055;0.002406;10.0057;10.0714;10.0589;10.0625;10.0669;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_woodburn-or-us.gml.20_52_63.apx.adf;0.018774;0.042192;10.0054;10.0051;10.0055;0.026428;0.005335;0.004332;0.001708;0.005375;10.0057;10.0055;10.0058;0.001484;10.0055;0.002042;0.004174;0.001997;0.005122;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_worcester-regional-transit-authority_20130618_1740.gml.50_80_181.apx.adf;0.133474;1.01593;10.0058;10.0058;10.0057;0.235415;0.01298;0.019349;0.005351;0.005859;10.0055;10.0057;10.0055;0.001715;10.0084;1.16913;1.09964;1.08761;1.12536;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_worcester-regional-transit-authority_20130618_1740.gml.80_80_214.apx.adf;2.86029;10.0071;10.0053;10.0058;10.0054;1.88814;0.007634;0.004748;0.002687;0.009681;10.0057;10.0056;10.0056;0.001734;10.0057;10.0824;10.0825;10.0823;10.083;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_yamhill-or-us.gml.20_157_241.apx.adf;10.0282;10.1088;10.0056;10.0061;10.0057;10.0441;0.024585;0.04812;0.004078;0.011487;10.0059;10.0056;10.0056;0.00183;10.0057;10.0839;10.0852;10.0835;10.1862;3;1200
adfgen_nacyc_se05_a_02_s_02_b_02_t_02_x_02_c_sXOR_Traffic_yuba-sutter-transit_20151216_1736.gml.20_216_327.apx.adf;10.0497;10.1124;10.0059;10.0058;10.0058;0.296927;0.041627;0.01107;0.004108;0.016061;10.0058;10.0059;10.0058;0.003066;10.0055;10.0926;10.0943;10.0986;10.0915
";
        let mut rdr = ReaderBuilder::new()
            .delimiter(b';')
            .has_headers(false)
            .from_reader(data.as_bytes());

        let imported = csv(
            &[
                None,
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Double),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::Float),
                Some(DataTypeName::U64),
                Some(DataTypeName::U64),
            ],
            &mut rdr,
        );

        assert!(imported.is_ok());
        assert_eq!(imported.as_ref().unwrap().len(), 21);
        assert_eq!(imported.as_ref().unwrap()[0].len(), 44);
    }
}
