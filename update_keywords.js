// ============================================================
// mongosh script – bulk update keyword1 & keyword2 by email
// Adjust collection name below if needed (default: 'staffs')
// Run: mongosh <connection-string> update_keywords.js
// ============================================================

const ops = [
  // Abi Tharappan (XH_16228)
  { updateOne: {
      filter: { email: { $regex: /^abitharappan@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Abidemi Akanji (XH_1710)
  { updateOne: {
      filter: { email: { $regex: /^dakanji@hotmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Adekunle Atanmuyide (XH_32203)
  { updateOne: {
      filter: { email: { $regex: /^adekunlejosh411@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Adeola Amusan (XH_3787)
  { updateOne: {
      filter: { email: { $regex: /^Adeniji450@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Adina Lupu (XH_32865)
  { updateOne: {
      filter: { email: { $regex: /^adina271072@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Adriana Miscov (XH_31076)
  { updateOne: {
      filter: { email: { $regex: /^danamiscov@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Agatha Osama-George (XH_20474)
  { updateOne: {
      filter: { email: { $regex: /^aglove_eno@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Ajith Nair (XH_32605)
  { updateOne: {
      filter: { email: { $regex: /^ajithviswanadhan118@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Palliative care" } }
  }},
  // Alint Vellanjiyil George (XH_32525)
  { updateOne: {
      filter: { email: { $regex: /^alint.george19@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Alison Lema (XH_14259)
  { updateOne: {
      filter: { email: { $regex: /^alplema94@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Amaka Ogbudo (XH_32559)
  { updateOne: {
      filter: { email: { $regex: /^ogbudoamaka@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Amenaghawon Osahon (XH_1382)
  { updateOne: {
      filter: { email: { $regex: /^osahonamen1@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Aminat Ajibade (XH_30031)
  { updateOne: {
      filter: { email: { $regex: /^Ajibadeaminat1020@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Anderson Chayikosa (XH_22555)
  { updateOne: {
      filter: { email: { $regex: /^achayikosa29@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Residential care" } }
  }},
  // Aneesh Mathew (XH_30875)
  { updateOne: {
      filter: { email: { $regex: /^aneeshmathew900@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Anik Eldrin Desai (XH_33012)
  { updateOne: {
      filter: { email: { $regex: /^desaieldrin95@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Anish Joy (XH_1584)
  { updateOne: {
      filter: { email: { $regex: /^anishjoy1234@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Anita Yorgova (XH_32621)
  { updateOne: {
      filter: { email: { $regex: /^anita_nz@abv.bg$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Anjeh Fongang (XH_21037)
  { updateOne: {
      filter: { email: { $regex: /^keltonakk@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Ann O' Shea (XH_2297)
  { updateOne: {
      filter: { email: { $regex: /^annoshea69@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Anna Rainey (XH_33010)
  { updateOne: {
      filter: { email: { $regex: /^annamrainey@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Anne Ngalapi (XH_5299)
  { updateOne: {
      filter: { email: { $regex: /^annengalapi@yahoo.ie$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Anu Pappachan (XH_30842)
  { updateOne: {
      filter: { email: { $regex: /^anupappimattoor@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Arjun Salim (XH_19741)
  { updateOne: {
      filter: { email: { $regex: /^arjunsalim1994@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Atinuke Ojo (XH_5072)
  { updateOne: {
      filter: { email: { $regex: /^ojoolamide63@yahoo.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Assisted feeding" } }
  }},
  // Awantu Anna Uyigue (XH_31289)
  { updateOne: {
      filter: { email: { $regex: /^awantuanna@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // AYODEJI KOLAPO BABATOLA (XH_2620)
  { updateOne: {
      filter: { email: { $regex: /^ayodejibabatola20@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Azeezat Alidu (XH_0534)
  { updateOne: {
      filter: { email: { $regex: /^aliduazeezat@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Assisted feeding" } }
  }},
  // Baiju Joseph (XH_32351)
  { updateOne: {
      filter: { email: { $regex: /^baijumannil@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Basirat Akinmoyede (XH_17860)
  { updateOne: {
      filter: { email: { $regex: /^akinmoyedebasirat@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Bino Kuriakose (XH_31420)
  { updateOne: {
      filter: { email: { $regex: /^mkuriakosebino@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Boineelo Ruele (XH_30222)
  { updateOne: {
      filter: { email: { $regex: /^boineeloruele@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Bolaji Ogiemwonyi (XH_30950)
  { updateOne: {
      filter: { email: { $regex: /^omolojabolaji@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Bright Peprah (XH_31117)
  { updateOne: {
      filter: { email: { $regex: /^samuelpep9@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Bunmi Olotu (XH_18906)
  { updateOne: {
      filter: { email: { $regex: /^bunmiolotu123@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Carolan Penders (XH_16137)
  { updateOne: {
      filter: { email: { $regex: /^macarolanprojimo@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Catherine Butler (XH_26431)
  { updateOne: {
      filter: { email: { $regex: /^cuckles50@gmail.com$/i } },
      update: { $set: { keyword1: "Assisted feeding", keyword2: "Manual handling" } }
  }},
  // Catherine Gogan (XH_31330)
  { updateOne: {
      filter: { email: { $regex: /^catgall@outlook.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Chamaine Thobekle Matonsi (XH_1853)
  { updateOne: {
      filter: { email: { $regex: /^charmainematonsi@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Charms Mammen (XH_32012)
  { updateOne: {
      filter: { email: { $regex: /^charmsmammen@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Chisom Okereke (XH_33302)
  { updateOne: {
      filter: { email: { $regex: /^juliet.chisom@outlook.ie$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Christian Obi (XH_32241)
  { updateOne: {
      filter: { email: { $regex: /^dilchris33@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Christian Wilson (XH_1516)
  { updateOne: {
      filter: { email: { $regex: /^Christianwilson43@yahoo.co.uk$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Christianne Mukuna (XH_0631)
  { updateOne: {
      filter: { email: { $regex: /^cmukuna97@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Christine Nakasote Chima (XH_0505)
  { updateOne: {
      filter: { email: { $regex: /^christinechima@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Chukuma ogwu (XH_15212)
  { updateOne: {
      filter: { email: { $regex: /^chukumaogwu@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Damien Larkin (XH_31104)
  { updateOne: {
      filter: { email: { $regex: /^damienlarkin1302@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Danelito Helena (XH_32427)
  { updateOne: {
      filter: { email: { $regex: /^dan.jozef226@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Danute Boguzaite (XH_31836)
  { updateOne: {
      filter: { email: { $regex: /^danaboguz16@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // David Aiguokhian (XH_31580)
  { updateOne: {
      filter: { email: { $regex: /^daiguokhian340@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Dawa Tsering (XH_18467)
  { updateOne: {
      filter: { email: { $regex: /^dawatsering1769@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Deepak P Dominic (XH_12057)
  { updateOne: {
      filter: { email: { $regex: /^deepakdominic3@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Denise O Reilly (XH_33089)
  { updateOne: {
      filter: { email: { $regex: /^oreillydee17@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Dimesha Kihaduwage (XH_33131)
  { updateOne: {
      filter: { email: { $regex: /^dimeshakularathna.dk@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Dingumuzi Ngono (XH_5699)
  { updateOne: {
      filter: { email: { $regex: /^justicengono@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Dipin Paul (XH_32975)
  { updateOne: {
      filter: { email: { $regex: /^dipinpaul15@email.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Edgar Livera (XH_32923)
  { updateOne: {
      filter: { email: { $regex: /^edgarlivera@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Eileen Lane (XH_32543)
  { updateOne: {
      filter: { email: { $regex: /^ebmlane@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Eldhose Jose (XH_31995)
  { updateOne: {
      filter: { email: { $regex: /^eldhojoin@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Palliative care" } }
  }},
  // Elizabeth Bosah (XH_32506)
  { updateOne: {
      filter: { email: { $regex: /^elizabethamakabosah1996@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Elizabeth Horgan (XH_1796)
  { updateOne: {
      filter: { email: { $regex: /^elizabeth.vtosa@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Eman Altoum (XH_2804)
  { updateOne: {
      filter: { email: { $regex: /^emanhashimaltoum1999@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Erana Ahamed Eva (XH_17628)
  { updateOne: {
      filter: { email: { $regex: /^evaerana55@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Mental health" } }
  }},
  // Eusebia Chimombe (XH_13843)
  { updateOne: {
      filter: { email: { $regex: /^eusebiachimombe27@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Faith Lowe (XH_32254)
  { updateOne: {
      filter: { email: { $regex: /^Faithlowe611@gmail.com$/i } },
      update: { $set: { keyword1: "Assisted feeding", keyword2: "Manual handling" } }
  }},
  // Falhada Abdi (XH_30880)
  { updateOne: {
      filter: { email: { $regex: /^falhadabdi10@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Assisted feeding" } }
  }},
  // Fathima Rani Ibrahimkutty (XH_14134)
  { updateOne: {
      filter: { email: { $regex: /^fathimaraniib@gmail.com$/i } },
      update: { $set: { keyword1: "Home care", keyword2: "Post-operative care" } }
  }},
  // Favour Chris (XH_32420)
  { updateOne: {
      filter: { email: { $regex: /^favourchris682@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Ferdinand Ngaobiwu (XH_32592)
  { updateOne: {
      filter: { email: { $regex: /^Ferdeve07@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Home care" } }
  }},
  // Fidelia Ameke (XH_18688)
  { updateOne: {
      filter: { email: { $regex: /^amekefidelia@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Fiona Kelly (XH_2182)
  { updateOne: {
      filter: { email: { $regex: /^fiomahony89@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Flevin Joseph (XH_32484)
  { updateOne: {
      filter: { email: { $regex: /^flevinkj67@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Freddy Mekang (XH_18401)
  { updateOne: {
      filter: { email: { $regex: /^mekangfreddy@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Home care" } }
  }},
  // Gabriel Thikan (XH_31515)
  { updateOne: {
      filter: { email: { $regex: /^gthikan@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Geethu Kuriakose (XH_30225)
  { updateOne: {
      filter: { email: { $regex: /^geethukuriakose.gk@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Gerard Lawless (XH_32987)
  { updateOne: {
      filter: { email: { $regex: /^j-law87@hotmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Gherlynn Jose (XH_32528)
  { updateOne: {
      filter: { email: { $regex: /^gherlynnjose@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Glory Olodu (XH_30723)
  { updateOne: {
      filter: { email: { $regex: /^iceglory@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Glory Peter (XH_30749)
  { updateOne: {
      filter: { email: { $regex: /^glorypeter76@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Hannah Jones (XH_30566)
  { updateOne: {
      filter: { email: { $regex: /^hannahmaijones00@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Home care" } }
  }},
  // Happiness Brown (XH_31918)
  { updateOne: {
      filter: { email: { $regex: /^Happinessbrownu@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Happy Vincent (XH_32457)
  { updateOne: {
      filter: { email: { $regex: /^imhappyvt1994@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Helen Egbo (XH_32822)
  { updateOne: {
      filter: { email: { $regex: /^helenotar@yahoo.ie$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Helen Omoregie (XH_33006)
  { updateOne: {
      filter: { email: { $regex: /^igberaeseonehiomhen@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Inoka Hewage (XH_30435)
  { updateOne: {
      filter: { email: { $regex: /^inokahewage900@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Isaac Ezeifedi (XH_22009)
  { updateOne: {
      filter: { email: { $regex: /^isaacezeifedi@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Itohan Irabor (XH_30639)
  { updateOne: {
      filter: { email: { $regex: /^margarettreasure27769@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Iyabo Adeniji (XH_4706)
  { updateOne: {
      filter: { email: { $regex: /^iyaboadeniji16@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Jan Paulose (XH_19447)
  { updateOne: {
      filter: { email: { $regex: /^janpaulose6@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Personal care" } }
  }},
  // Jayson Addatu (XH_32642)
  { updateOne: {
      filter: { email: { $regex: /^jaysonaddatu17@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Jennien O'Gorman (XH_27394)
  { updateOne: {
      filter: { email: { $regex: /^jennienog@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Palliative care" } }
  }},
  // Jennifer Collins (XH_30848)
  { updateOne: {
      filter: { email: { $regex: /^jennifermarycollins1968@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Home care" } }
  }},
  // Jephthah Okpowe (XH_18109)
  { updateOne: {
      filter: { email: { $regex: /^Jephthahokpowe@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Assisted feeding" } }
  }},
  // Jessica collins (XH_10164)
  { updateOne: {
      filter: { email: { $regex: /^118429026@umail.ucc.ie$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Jijo Varghese (XH_32564)
  { updateOne: {
      filter: { email: { $regex: /^jijovarghese658@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Jinesh Poulose (XH_6581)
  { updateOne: {
      filter: { email: { $regex: /^jineshpoulose2010@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Palliative care" } }
  }},
  // Jis Joseph (XH_27231)
  { updateOne: {
      filter: { email: { $regex: /^jisj282@gmail.com$/i } },
      update: { $set: { keyword1: "Palliative care", keyword2: "Manual handling" } }
  }},
  // Jobil Jose (XH_31483)
  { updateOne: {
      filter: { email: { $regex: /^jobilvjose@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Jobin Mathew Alingal (XH_16941)
  { updateOne: {
      filter: { email: { $regex: /^jobin.ma99@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Manual handling" } }
  }},
  // Jobish Thomas (XH_31692)
  { updateOne: {
      filter: { email: { $regex: /^jobishnthomas@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Palliative care" } }
  }},
  // Joji George (XH_32651)
  { updateOne: {
      filter: { email: { $regex: /^jojigeorge147@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Joyce Lawal (XH_32526)
  { updateOne: {
      filter: { email: { $regex: /^joycelawal10@gmail.com$/i } },
      update: { $set: { keyword1: "Assisted feeding", keyword2: "Manual handling" } }
  }},
  // Judith Moyo (XH_20491)
  { updateOne: {
      filter: { email: { $regex: /^judithndimande13@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Julius E Omoruyi (XH_32996)
  { updateOne: {
      filter: { email: { $regex: /^Jomoruyi69@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Home care" } }
  }},
  // Justin Francis (XH_31424)
  { updateOne: {
      filter: { email: { $regex: /^justinsudhakar134@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Justina Eberechi (XH_31066)
  { updateOne: {
      filter: { email: { $regex: /^ejustinaijeoma83@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Justina Oludayomi (XH_33243)
  { updateOne: {
      filter: { email: { $regex: /^justina.oludayomi@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Chronic illness management" } }
  }},
  // Kaenat Nawaz (XH_0270)
  { updateOne: {
      filter: { email: { $regex: /^kaenatnawaz@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Assisted feeding" } }
  }},
  // Kambaji Muboyayi (XH_31244)
  { updateOne: {
      filter: { email: { $regex: /^chantalkambaji25@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Katelynn Walsh (XH_30968)
  { updateOne: {
      filter: { email: { $regex: /^Katelynnwalsh17092004@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Kehinde Akintola (XH_20073)
  { updateOne: {
      filter: { email: { $regex: /^kenny67adeyemi@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // khalid Adams (XH_14770)
  { updateOne: {
      filter: { email: { $regex: /^adamskhalid84@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Koliver Kadimashi (XH_33067)
  { updateOne: {
      filter: { email: { $regex: /^koliverkadimashi@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Kudzi Mangwandi (XH_1803)
  { updateOne: {
      filter: { email: { $regex: /^kudzimangwandi@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Kunjumon Kanneth Mathai (XH_11585)
  { updateOne: {
      filter: { email: { $regex: /^kunjumonkanneth71@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Laeticia Njoku (XH_31677)
  { updateOne: {
      filter: { email: { $regex: /^laeticianjoku@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Laju Mikie (XH_11394)
  { updateOne: {
      filter: { email: { $regex: /^Mikielaju2016@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Latifat O Adebowale (XH_2082)
  { updateOne: {
      filter: { email: { $regex: /^latrex09@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Laurepm Okolie (XH_2198)
  { updateOne: {
      filter: { email: { $regex: /^laurepmbesem@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Leticia Tchatat (XH_31567)
  { updateOne: {
      filter: { email: { $regex: /^tchatatleticia@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Lijomon Joseph (XH_7254)
  { updateOne: {
      filter: { email: { $regex: /^lijomonjoseph01@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Liju Joseph (XH_31526)
  { updateOne: {
      filter: { email: { $regex: /^lijooputhuva@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Loving Omote (XH_22672)
  { updateOne: {
      filter: { email: { $regex: /^lovingomote@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Lucy Mbasa (XH_30798)
  { updateOne: {
      filter: { email: { $regex: /^dineo72133899@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Mabel Onwukwe (XH_3808)
  { updateOne: {
      filter: { email: { $regex: /^mabel.onwukwe@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Madelaine Guemba (XH_2192)
  { updateOne: {
      filter: { email: { $regex: /^guembamadelaine@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Mamie Pongo Ngonde (XH_3320)
  { updateOne: {
      filter: { email: { $regex: /^pongongonde2@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Manoj James (XH_32781)
  { updateOne: {
      filter: { email: { $regex: /^manojjames250@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Manoj Prabakaran (XH_30755)
  { updateOne: {
      filter: { email: { $regex: /^manojprabakaranr@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Manu Muraleedharan (XH_32776)
  { updateOne: {
      filter: { email: { $regex: /^nellimukal@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Mental health" } }
  }},
  // Manuel Mannarath Dominic (XH_30622)
  { updateOne: {
      filter: { email: { $regex: /^manuelmannarath@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Maria Phelan (XH_1034)
  { updateOne: {
      filter: { email: { $regex: /^mariaphelan1@outlook.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Maria Snyman (XH_5358)
  { updateOne: {
      filter: { email: { $regex: /^snymanclan@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Personal care" } }
  }},
  // Martin Lazo (XH_30216)
  { updateOne: {
      filter: { email: { $regex: /^martinlazo50@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Maxwell Musareva (XH_17359)
  { updateOne: {
      filter: { email: { $regex: /^maxwellmusareva@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Mbiya Ngandu (XH_32424)
  { updateOne: {
      filter: { email: { $regex: /^theongandu43@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Mercy Abah (XH_31952)
  { updateOne: {
      filter: { email: { $regex: /^mercyaba797@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Michelle Mbalenhle Ndlovu (XH_1134)
  { updateOne: {
      filter: { email: { $regex: /^ndlovumichelle96@gmail.com$/i } },
      update: { $set: { keyword1: "Home care", keyword2: "Manual handling" } }
  }},
  // Midhun Vijay (XH_32635)
  { updateOne: {
      filter: { email: { $regex: /^midhunvijay44@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Mir Karimullah Zia (XH_30136)
  { updateOne: {
      filter: { email: { $regex: /^mirkarimullah.zia@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Mireille Kimfuta (XH_24719)
  { updateOne: {
      filter: { email: { $regex: /^miesi.mireille.1977@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Mistura Odofin (XH_31965)
  { updateOne: {
      filter: { email: { $regex: /^omistura4@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Mohammed Khan (XH_32298)
  { updateOne: {
      filter: { email: { $regex: /^starlightfiji@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Monika Sagi (XH_1660)
  { updateOne: {
      filter: { email: { $regex: /^sagimoni@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Mpho Asteria Masasa (XH_32141)
  { updateOne: {
      filter: { email: { $regex: /^masasaasteria79@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Mujinga Yombo (XH_32072)
  { updateOne: {
      filter: { email: { $regex: /^yombos31@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Palliative care" } }
  }},
  // Naomi Gwesere (XH_30582)
  { updateOne: {
      filter: { email: { $regex: /^naomigwesere@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Ndidi Okpe (XH_30721)
  { updateOne: {
      filter: { email: { $regex: /^ndidiokpe5@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Nelson Odhigbo (XH_31442)
  { updateOne: {
      filter: { email: { $regex: /^vamilo@hotmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Nene Luany (XH_6809)
  { updateOne: {
      filter: { email: { $regex: /^nbatshi06@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // NEO GIFT MOLEFHE (XH_3261)
  { updateOne: {
      filter: { email: { $regex: /^ngmolefhe@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Neo Kwada (XH_32362)
  { updateOne: {
      filter: { email: { $regex: /^neohkwadah@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Assisted feeding" } }
  }},
  // Nibu Ramathuparambil (XH_32502)
  { updateOne: {
      filter: { email: { $regex: /^nibuvr30@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Personal care" } }
  }},
  // Nikola Muza (XH_7330)
  { updateOne: {
      filter: { email: { $regex: /^nikolamuza05@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Nimi Christopher (XH_30727)
  { updateOne: {
      filter: { email: { $regex: /^nimichris123@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Nobuhle Tracy Codesa Mfeka (XH_20290)
  { updateOne: {
      filter: { email: { $regex: /^nobuhletracy22@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Nokukhanya Banda (XH_31962)
  { updateOne: {
      filter: { email: { $regex: /^bandakhanyie3@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Home care" } }
  }},
  // Nomsa Cindy Malaza (XH_17328)
  { updateOne: {
      filter: { email: { $regex: /^cindymalaza1@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Nora Nemeth (XH_11236)
  { updateOne: {
      filter: { email: { $regex: /^nemeth.nora80@hotmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Personal care" } }
  }},
  // Nthabiseng Tsie (XH_29923)
  { updateOne: {
      filter: { email: { $regex: /^Julliettsie@yahoo.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Assisted feeding" } }
  }},
  // Nurat Omisore (XH_30378)
  { updateOne: {
      filter: { email: { $regex: /^omisorenurat15@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Obianuju Ezenna (XH_23354)
  { updateOne: {
      filter: { email: { $regex: /^ujuezenna@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Ojore Abdul (XH_32831)
  { updateOne: {
      filter: { email: { $regex: /^ojorelwe@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Oladotun Oluwatunmise Temilola (XH_1523)
  { updateOne: {
      filter: { email: { $regex: /^tonyteetunlapa@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Olatunde Emmanuel (XH_21019)
  { updateOne: {
      filter: { email: { $regex: /^emmatunde61@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Omosalewa Idowu Akindele (XH_1921)
  { updateOne: {
      filter: { email: { $regex: /^salewaabeke@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Omotope Babalol (XH_20244)
  { updateOne: {
      filter: { email: { $regex: /^omotopebabalola@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Ope Rasheedat Raji (XH_1571)
  { updateOne: {
      filter: { email: { $regex: /^operaajji@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Oyemike Patrick Ossai (XH_31558)
  { updateOne: {
      filter: { email: { $regex: /^patrickossai1972@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Pamella Sibindi (XH_32754)
  { updateOne: {
      filter: { email: { $regex: /^mvelasegerald@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Patricia Kugarahutsva (XH_23289)
  { updateOne: {
      filter: { email: { $regex: /^patriciakugarahutsva@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Phibin Ninan (XH_31588)
  { updateOne: {
      filter: { email: { $regex: /^phibinnainan88@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Phumulani Ndlovu (XH_2350)
  { updateOne: {
      filter: { email: { $regex: /^phumumthunzi@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Pradeep Raj (XH_31988)
  { updateOne: {
      filter: { email: { $regex: /^pradeepraj8647@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Precious Edegbe (XH_21443)
  { updateOne: {
      filter: { email: { $regex: /^preciousedegbe1@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Prince John (XH_8245)
  { updateOne: {
      filter: { email: { $regex: /^princejohn708@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Princess Isibor (XH_33412)
  { updateOne: {
      filter: { email: { $regex: /^princessisibor@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Priscilla Ofoe (XH_31943)
  { updateOne: {
      filter: { email: { $regex: /^prettynelson1993@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Progress Halliday (XH_31909)
  { updateOne: {
      filter: { email: { $regex: /^hallycrew@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Ratheesh Kumar (XH_22562)
  { updateOne: {
      filter: { email: { $regex: /^ratheeshrnair52@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Manual handling" } }
  }},
  // Raul Maftei (XH_30405)
  { updateOne: {
      filter: { email: { $regex: /^mafteiraul@gmail.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Home care" } }
  }},
  // Real Nkomo (XH_30877)
  { updateOne: {
      filter: { email: { $regex: /^rqn1305@gmail.com$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Mental health" } }
  }},
  // Rensie Degollado (XH_30398)
  { updateOne: {
      filter: { email: { $regex: /^rensiejohn19@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Ribin Mathew (XH_22847)
  { updateOne: {
      filter: { email: { $regex: /^mathewribin@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Home care" } }
  }},
  // Robin George (XH_33406)
  { updateOne: {
      filter: { email: { $regex: /^grobin49@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Robin Jose (XH_31715)
  { updateOne: {
      filter: { email: { $regex: /^robin4may29@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Roseline Hussein (XH_32739)
  { updateOne: {
      filter: { email: { $regex: /^roselinehussline75@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Sajana Acharya (XH_23648)
  { updateOne: {
      filter: { email: { $regex: /^naturekrishna@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Saju Paul (XH_32661)
  { updateOne: {
      filter: { email: { $regex: /^sajupaul2009@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // sakhile sithole (XH_14023)
  { updateOne: {
      filter: { email: { $regex: /^sakhilesithole6782@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Palliative care" } }
  }},
  // Samkeliso Ndlovu (XH_31726)
  { updateOne: {
      filter: { email: { $regex: /^nsamkeliso42@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Sean Makalima (XH_22862)
  { updateOne: {
      filter: { email: { $regex: /^sean_makalima@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Sebastian Baby (XH_32785)
  { updateOne: {
      filter: { email: { $regex: /^sebastianbaby2@gmail.com$/i } },
      update: { $set: { keyword1: "Assisted feeding", keyword2: "Manual handling" } }
  }},
  // Sefinat Oyindamola Adams (XH_30085)
  { updateOne: {
      filter: { email: { $regex: /^adamssefinat@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Manual handling" } }
  }},
  // Semadulo Omponye Boko (XH_2760)
  { updateOne: {
      filter: { email: { $regex: /^bokosemadulo@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Setinamavis Osuji (XH_30201)
  { updateOne: {
      filter: { email: { $regex: /^ihukobiqueenchidinma@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Assisted feeding" } }
  }},
  // Shakeel Mohammad Foyjun (XH_1845)
  { updateOne: {
      filter: { email: { $regex: /^shaks32@hotmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Shannon Mc Daid (XH_32712)
  { updateOne: {
      filter: { email: { $regex: /^shannonmd05@icloud.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Sharon Ndlovu (XH_14916)
  { updateOne: {
      filter: { email: { $regex: /^ndlovusharon94@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Shedrack Patrick (XH_31527)
  { updateOne: {
      filter: { email: { $regex: /^sheddy2020@outlook.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Acute hospital wards" } }
  }},
  // Shiju Abraham (XH_2743)
  { updateOne: {
      filter: { email: { $regex: /^shijukvl82@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Sinead Wardle (XH_33299)
  { updateOne: {
      filter: { email: { $regex: /^sineadwardlex123@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Assisted feeding" } }
  }},
  // Siobhan Ryan (XH_30127)
  { updateOne: {
      filter: { email: { $regex: /^siobhanryangrufferty@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Home care" } }
  }},
  // Sonu Joseph (XH_32638)
  { updateOne: {
      filter: { email: { $regex: /^sonujoseph744@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Palliative care" } }
  }},
  // Subin Benny (XH_11077)
  { updateOne: {
      filter: { email: { $regex: /^subinbenny3@gmail.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Post-operative care" } }
  }},
  // Success Igwegbe (XH_32858)
  { updateOne: {
      filter: { email: { $regex: /^successigwegbe@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Suchana Khadka (XH_30055)
  { updateOne: {
      filter: { email: { $regex: /^suchana.khd@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Sulaiman Olafiranye (XH_1559)
  { updateOne: {
      filter: { email: { $regex: /^Olafiranyesulaiman@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Sunil Mathew (XH_30065)
  { updateOne: {
      filter: { email: { $regex: /^sunilmathew05@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Sunitha Cherooth (XH_31513)
  { updateOne: {
      filter: { email: { $regex: /^sunithac889@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Personal care" } }
  }},
  // Taofeek Ibrahim Olawale (XH_0380)
  { updateOne: {
      filter: { email: { $regex: /^olawaleibrahim619@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Tatenda Chivengwa (XH_32438)
  { updateOne: {
      filter: { email: { $regex: /^tatschiv@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Mental health" } }
  }},
  // Temple Isigwe (XH_31262)
  { updateOne: {
      filter: { email: { $regex: /^templeisigwe@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Thierry Mbambu (XH_11322)
  { updateOne: {
      filter: { email: { $regex: /^Mbambuthierry@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Thomas Sekoni (XH_0743)
  { updateOne: {
      filter: { email: { $regex: /^thomascheaques86@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Tigi Mathew (XH_31612)
  { updateOne: {
      filter: { email: { $regex: /^tigimathews@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Tom V Mathew (XH_30608)
  { updateOne: {
      filter: { email: { $regex: /^tomvmathew000@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Post-operative care" } }
  }},
  // Torkia Mekhtiche (XH_30656)
  { updateOne: {
      filter: { email: { $regex: /^torkia.mekhtich@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Tovias Siska (XH_5841)
  { updateOne: {
      filter: { email: { $regex: /^tovias.siska1976@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Toyin Alabi (XH_29783)
  { updateOne: {
      filter: { email: { $regex: /^toyinalabi91@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Valdirene Galli (XH_31382)
  { updateOne: {
      filter: { email: { $regex: /^vallgalli@hotmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Valentine Harawa (XH_1114)
  { updateOne: {
      filter: { email: { $regex: /^valentineharawa.vh@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Varghese Aliyas (XH_30828)
  { updateOne: {
      filter: { email: { $regex: /^linualiyas@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Victor Ofoegbu (XH_2014)
  { updateOne: {
      filter: { email: { $regex: /^doniyk147@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Assisted feeding" } }
  }},
  // Vishala Mathipela (XH_32940)
  { updateOne: {
      filter: { email: { $regex: /^vishalamathipela9553@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Vishnu Janardanan (XH_21566)
  { updateOne: {
      filter: { email: { $regex: /^vishnupj169@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Vishnu Jayadev (XH_0173)
  { updateOne: {
      filter: { email: { $regex: /^vishnujayadev399@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Williams Musa Thaddeus (XH_31045)
  { updateOne: {
      filter: { email: { $regex: /^willythaddy.62@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Yinka Fatai Ayanleye (XH_2105)
  { updateOne: {
      filter: { email: { $regex: /^advaloremnig@yahoo.co.uk$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Yuhan Liu (XH_30465)
  { updateOne: {
      filter: { email: { $regex: /^claireblairliuyuhan@outlook.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Yuliia Sotnyk (XH_31498)
  { updateOne: {
      filter: { email: { $regex: /^sotnik.july@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Zubair Ahmad (XH_11243)
  { updateOne: {
      filter: { email: { $regex: /^harismahmood400@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Abdul Koyan (XH_32167)
  { updateOne: {
      filter: { email: { $regex: /^sahad2421@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Abisola Fasan (XH_30890)
  { updateOne: {
      filter: { email: { $regex: /^abisolafasan@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Mental health" } }
  }},
  // Adedoyin Erhabor (XH_30151)
  { updateOne: {
      filter: { email: { $regex: /^adedoyinerhabor@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Special observation" } }
  }},
  // Adejumoke Adegboye (XH_18735)
  { updateOne: {
      filter: { email: { $regex: /^jumokeadegboye@gmail.com$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Assisted feeding" } }
  }},
  // Adepeju Odunola Adeyemi (XH_9182)
  { updateOne: {
      filter: { email: { $regex: /^temilove2002@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Palliative care" } }
  }},
  // Adrian Urbanski (XH_0753)
  { updateOne: {
      filter: { email: { $regex: /^adrianurbanski0@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Elderly care" } }
  }},
  // Aedin O'Gorman (XH_25757)
  { updateOne: {
      filter: { email: { $regex: /^ogormanaedin@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Manual handling" } }
  }},
  // Agnes Lawani (XH_0366)
  { updateOne: {
      filter: { email: { $regex: /^agneslawani@gmail.com$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Personal care" } }
  }},
  // Aisling Denihan (XH_32018)
  { updateOne: {
      filter: { email: { $regex: /^aislingdenihan@yahoo.ie$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Residential care" } }
  }},
  // Akash Koshy (XH_24395)
  { updateOne: {
      filter: { email: { $regex: /^akashckoshy17@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Post-operative care" } }
  }},
  // Akhila Soman (XH_31218)
  { updateOne: {
      filter: { email: { $regex: /^akhilaisoman@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Akhildev Mundapurath ()
  { updateOne: {
      filter: { email: { $regex: /^akhildevwyd@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Alannah Lenihan (XH_32887)
  { updateOne: {
      filter: { email: { $regex: /^allenihan12@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Palliative care" } }
  }},
  // Alessandra silva ()
  { updateOne: {
      filter: { email: { $regex: /^alealvesadvgo@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Alexandra Maria Lungu (XH_0142)
  { updateOne: {
      filter: { email: { $regex: /^alexandralungu43@yahoo.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Residential care" } }
  }},
  // Alexandra Zaharia (XH_33213)
  { updateOne: {
      filter: { email: { $regex: /^contesa_ea@yahoo.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Residential care" } }
  }},
  // Alexandru Andres (XH_32492)
  { updateOne: {
      filter: { email: { $regex: /^alexandru210384@gmail.com$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Residential care" } }
  }},
  // Alice Dube (XH_1525)
  { updateOne: {
      filter: { email: { $regex: /^alicedube85@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Amanda Nobrega (XH_31882)
  { updateOne: {
      filter: { email: { $regex: /^amanda.g.enfermagem@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Amy Hallissey ()
  { updateOne: {
      filter: { email: { $regex: /^hallisseyamy@icloud.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Amy Hegarty ()
  { updateOne: {
      filter: { email: { $regex: /^amyhegarty1@gmail.com$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Home care" } }
  }},
  // Ana Barbu (XH_31385)
  { updateOne: {
      filter: { email: { $regex: /^barbumaria611@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Home care" } }
  }},
  // Ana Maria Pop ()
  { updateOne: {
      filter: { email: { $regex: /^marryann1111@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Ana Maria Stanescu (XH_24336)
  { updateOne: {
      filter: { email: { $regex: /^anamaria.stanescu.ams@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Aneesh Abraham (XH_32743)
  { updateOne: {
      filter: { email: { $regex: /^aneeshabrahamkj@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Aneeta Thenumakkil Varghese (XH_17762)
  { updateOne: {
      filter: { email: { $regex: /^aneetavarghese093@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Angela Bento (XH_32225)
  { updateOne: {
      filter: { email: { $regex: /^angelabento@umail.ucc.ie$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Chronic illness management" } }
  }},
  // Anil Joseph ()
  { updateOne: {
      filter: { email: { $regex: /^anilandrea007@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Anish Kurian (XH_30951)
  { updateOne: {
      filter: { email: { $regex: /^anishkurian9@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Anjali Gopan (XH_17166)
  { updateOne: {
      filter: { email: { $regex: /^gopananju94@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Manual handling" } }
  }},
  // Anjitha Jai (XH_30713)
  { updateOne: {
      filter: { email: { $regex: /^anjithajaimaniyattu123@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Chronic illness management" } }
  }},
  // Anju Alphonsa Mathew (XH_3560)
  { updateOne: {
      filter: { email: { $regex: /^anjualphonsam@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Anju John (XH_32204)
  { updateOne: {
      filter: { email: { $regex: /^geniusanju@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Anju Ramachandran Nair (XH_32847)
  { updateOne: {
      filter: { email: { $regex: /^anjurama1992@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Ankitha Jose (XH_30660)
  { updateOne: {
      filter: { email: { $regex: /^ankitha.pink@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Anna Breen (XH_31468)
  { updateOne: {
      filter: { email: { $regex: /^breenanna00@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Mental health" } }
  }},
  // Anna Higgins (XH_31172)
  { updateOne: {
      filter: { email: { $regex: /^anna.higgins1995@hotmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Annah Bangi ()
  { updateOne: {
      filter: { email: { $regex: /^annahbg10@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Annatolia Gava (XH_19261)
  { updateOne: {
      filter: { email: { $regex: /^annatoliagava74@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Chronic illness management" } }
  }},
  // Anne Horgan (XH_9782)
  { updateOne: {
      filter: { email: { $regex: /^annehorgan22@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Anne Metcalfe (XH_30680)
  { updateOne: {
      filter: { email: { $regex: /^Annamae73@icloud.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Anne Teresa Hussey Pattinson (XH_18651)
  { updateOne: {
      filter: { email: { $regex: /^annepattinson8@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Annmarie Lacuarta (XH_27664)
  { updateOne: {
      filter: { email: { $regex: /^ann.lacuarta@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Palliative care" } }
  }},
  // Anoop Purushothaman (XH_32627)
  { updateOne: {
      filter: { email: { $regex: /^anooptp358@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Ansar Ali (XH_32333)
  { updateOne: {
      filter: { email: { $regex: /^ansaralilabba@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Anu Karottethadam Raju (XH_2589)
  { updateOne: {
      filter: { email: { $regex: /^anukraju2392@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Anusha ojha (XH_3598)
  { updateOne: {
      filter: { email: { $regex: /^anuozha5@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Aoife Dermody (XH_27187)
  { updateOne: {
      filter: { email: { $regex: /^Aoifedermody13@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Archa Bindu Nair (XH_1131)
  { updateOne: {
      filter: { email: { $regex: /^archabnair1995@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Arian Alexis Remolar (XH_10512)
  { updateOne: {
      filter: { email: { $regex: /^arianremolar2006@yahoo.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Post-operative care" } }
  }},
  // Arjun Mohan (XH_32958)
  { updateOne: {
      filter: { email: { $regex: /^arjunmohan693@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Emergency department" } }
  }},
  // Arogia Mary Diviyanathan (XH_33297)
  { updateOne: {
      filter: { email: { $regex: /^arogiamary7@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Manual handling" } }
  }},
  // Arsha Babichan (XH_15000)
  { updateOne: {
      filter: { email: { $regex: /^arshacibin@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Special observation" } }
  }},
  // Arya Varghese (XH_15205)
  { updateOne: {
      filter: { email: { $regex: /^varghesedaughter@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Asha Baburaj (XH_24030)
  { updateOne: {
      filter: { email: { $regex: /^ashaelizabeth702@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Asha Mathew ()
  { updateOne: {
      filter: { email: { $regex: /^ashamathew089@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Athira Raju ()
  { updateOne: {
      filter: { email: { $regex: /^athirapratheesh2023@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Personal care" } }
  }},
  // Avril Delaney (XH_32707)
  { updateOne: {
      filter: { email: { $regex: /^avrild92@hotmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Avril Kelly (XH_32346)
  { updateOne: {
      filter: { email: { $regex: /^kellyavril@hotmail.co.uk$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Barbara Maria Janiec (XH_2585)
  { updateOne: {
      filter: { email: { $regex: /^barbarajaniec@ymail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Barbara Moran (XH_32951)
  { updateOne: {
      filter: { email: { $regex: /^barbararose58@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Barbara Ruggeri (XH_29955)
  { updateOne: {
      filter: { email: { $regex: /^angelmarg1@icloud.com$/i } },
      update: { $set: { keyword1: "Palliative care", keyword2: "Mental health" } }
  }},
  // Belinda Gyasi (XH_30718)
  { updateOne: {
      filter: { email: { $regex: /^blindajessy@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Special observation" } }
  }},
  // Belinda Mandaza (XH_32750)
  { updateOne: {
      filter: { email: { $regex: /^belindamandaza1@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Belisha Bethel Dmello (XH_21016)
  { updateOne: {
      filter: { email: { $regex: /^bd.belisha95@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Special observation" } }
  }},
  // Bestine Mathew (XH_14407)
  { updateOne: {
      filter: { email: { $regex: /^bestinerockhead@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Bianca Moise (XH_23981)
  { updateOne: {
      filter: { email: { $regex: /^dogariu.bianca@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Biji Gopalakrishnan (XH_1396)
  { updateOne: {
      filter: { email: { $regex: /^bijivyppil@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Special observation" } }
  }},
  // Bini J Kumminithottathil (XH_30937)
  { updateOne: {
      filter: { email: { $regex: /^binithomas@hotmail.it$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Elderly care" } }
  }},
  // Binoy George ()
  { updateOne: {
      filter: { email: { $regex: /^binoygm1@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Binsa M Jose (XH_31696)
  { updateOne: {
      filter: { email: { $regex: /^binsamaryjose@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Binumon Kochunny (XH_31231)
  { updateOne: {
      filter: { email: { $regex: /^binumonck@yahoo.ie$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Blassy Beeby (XH_33022)
  { updateOne: {
      filter: { email: { $regex: /^blassybeeby@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Blessan Chacko Joseph (XH_7936)
  { updateOne: {
      filter: { email: { $regex: /^christsgospel@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Palliative care" } }
  }},
  // Blessings Chatsama (XH_4344)
  { updateOne: {
      filter: { email: { $regex: /^beechatsama@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Blessy Shery (XH_32615)
  { updateOne: {
      filter: { email: { $regex: /^Blessyshery111@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Boitshwarelo Mosotho (XH_32024)
  { updateOne: {
      filter: { email: { $regex: /^boitshwarelo.bm.bm@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Mental health" } }
  }},
  // Botshelo Rentse (XH_21251)
  { updateOne: {
      filter: { email: { $regex: /^botshelo31@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Brenda Chipo Sibanda (XH_2838)
  { updateOne: {
      filter: { email: { $regex: /^sibandabrenda29@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Brian Long (XH_1101)
  { updateOne: {
      filter: { email: { $regex: /^brianlong1990@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Caoimhe Halpenny (XH_1483)
  { updateOne: {
      filter: { email: { $regex: /^chalpenn@tcd.ie$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Carol Anne Craig (XH_1925)
  { updateOne: {
      filter: { email: { $regex: /^CAROLFINN1987@HOTMAIL.COM$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Elderly care" } }
  }},
  // Caroline Fitzgerald (XH_32426)
  { updateOne: {
      filter: { email: { $regex: /^carfitz38@icloud.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Caroline Lynch ()
  { updateOne: {
      filter: { email: { $regex: /^lynch.caroline74@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Catherine Orji (XH_23551)
  { updateOne: {
      filter: { email: { $regex: /^divineheritage49@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Mental health" } }
  }},
  // Cathy Phillips (XH_31184)
  { updateOne: {
      filter: { email: { $regex: /^annc.phillips@hse.ie$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Palliative care" } }
  }},
  // Celine Mc Parland (XH_32168)
  { updateOne: {
      filter: { email: { $regex: /^cpmcparland@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Charity Nwoke (XH_30491)
  { updateOne: {
      filter: { email: { $regex: /^ndubuisicharity826@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Charles Bett (XH_32566)
  { updateOne: {
      filter: { email: { $regex: /^carlbett678@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Cherie Arniego (XH_30640)
  { updateOne: {
      filter: { email: { $regex: /^cherie_deasis@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Chidera Maduka ()
  { updateOne: {
      filter: { email: { $regex: /^invincibledera1@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Intellectual disability" } }
  }},
  // Chidera Nnabuenyi (XH_31014)
  { updateOne: {
      filter: { email: { $regex: /^nnabuenyichidera6@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Chinchu Kuruvilla (XH_31271)
  { updateOne: {
      filter: { email: { $regex: /^chinchukuruvilla879@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Chinthu Prakash (XH_13571)
  { updateOne: {
      filter: { email: { $regex: /^chinthuprakash68@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Elderly care" } }
  }},
  // Chioma Oparah ()
  { updateOne: {
      filter: { email: { $regex: /^chiomaoparah3@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Chipo Makahamadze (XH_33138)
  { updateOne: {
      filter: { email: { $regex: /^chipocharlot@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Manual handling" } }
  }},
  // Christeena Joy (XH_30383)
  { updateOne: {
      filter: { email: { $regex: /^christymanu108@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Christo Sailam (XH_31912)
  { updateOne: {
      filter: { email: { $regex: /^scmr4898@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Special observation" } }
  }},
  // Christo Varghese (XH_1025)
  { updateOne: {
      filter: { email: { $regex: /^christovarghese.644@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Chukwuma Udensi (XH_0905)
  { updateOne: {
      filter: { email: { $regex: /^chuksudensi@hotmail.com$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Mental health" } }
  }},
  // Ciara Moloney ()
  { updateOne: {
      filter: { email: { $regex: /^moloneyciara12@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Ciara Whyte (XH_32665)
  { updateOne: {
      filter: { email: { $regex: /^ciarawhyte4@hotmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Ciju Joseph (XH_29750)
  { updateOne: {
      filter: { email: { $regex: /^kokkenciju@yahoo.co.in$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Cora Hennigan (XH_31312)
  { updateOne: {
      filter: { email: { $regex: /^corahennigan@ymail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Courage Omoigui (XH_30990)
  { updateOne: {
      filter: { email: { $regex: /^omoiguicourage1@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Manual handling" } }
  }},
  // Cousemore Muchena (XH_8148)
  { updateOne: {
      filter: { email: { $regex: /^cozie_much@yahoo.co.uk$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Damilola Odewade (XH_31228)
  { updateOne: {
      filter: { email: { $regex: /^damiodewade@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Dan Supangan (XH_32432)
  { updateOne: {
      filter: { email: { $regex: /^dansupangan@aol.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Elderly care" } }
  }},
  // Deborah Fitzpatrick (XH_20109)
  { updateOne: {
      filter: { email: { $regex: /^deborahfitzpatrick1999@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Residential care" } }
  }},
  // Deborah Murphy (XH_32142)
  { updateOne: {
      filter: { email: { $regex: /^deby_709@hotmail.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Assisted feeding" } }
  }},
  // Deepthimol Dayanandan (XH_13740)
  { updateOne: {
      filter: { email: { $regex: /^dharasenthil17@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Deirdre Murphy (XH_31393)
  { updateOne: {
      filter: { email: { $regex: /^deirdremurphysara1@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Denise Pearson ()
  { updateOne: {
      filter: { email: { $regex: /^mumsy50@live.co.uk$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Dhanya Ulahannan ()
  { updateOne: {
      filter: { email: { $regex: /^dhanupu582@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Diksha Rani (XH_31816)
  { updateOne: {
      filter: { email: { $regex: /^lesetta.leslie@cosgravenursinghomes.ie$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Dineshkumar Sampath ()
  { updateOne: {
      filter: { email: { $regex: /^dineshkumar.june89@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Dinnu Baby (XH_30012)
  { updateOne: {
      filter: { email: { $regex: /^dinnubaby1996@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Dino Doddy (XH_31889)
  { updateOne: {
      filter: { email: { $regex: /^dinododdy@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Divya Varghese (XH_31782)
  { updateOne: {
      filter: { email: { $regex: /^divyaelizabeth396@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Doinita Tatiana Barbu (XH_5978)
  { updateOne: {
      filter: { email: { $regex: /^barbutatiana.ie@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Dominika Antczak (XH_31193)
  { updateOne: {
      filter: { email: { $regex: /^dominikaa117@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Residential care" } }
  }},
  // Dona Tomy (XH_32681)
  { updateOne: {
      filter: { email: { $regex: /^donatomy21@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Manual handling" } }
  }},
  // Donald Diggah (XH_32101)
  { updateOne: {
      filter: { email: { $regex: /^donalddiggah@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Elderly care" } }
  }},
  // Dorathy Nnebe (XH_32331)
  { updateOne: {
      filter: { email: { $regex: /^dorathynnebe@gmail.com$/i } },
      update: { $set: { keyword1: "PEG feeding", keyword2: "Assisted feeding" } }
  }},
  // Doreen Ngaku (XH_32263)
  { updateOne: {
      filter: { email: { $regex: /^dorren.ngaku@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Drastic Muchena (XH_32257)
  { updateOne: {
      filter: { email: { $regex: /^dmuchena88@gmail.com$/i } },
      update: { $set: { keyword1: "Home care", keyword2: "Chronic illness management" } }
  }},
  // Dudzai Muzwimwe (XH_31869)
  { updateOne: {
      filter: { email: { $regex: /^dmuzvimwe52@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Edcilin Silva (XH_30082)
  { updateOne: {
      filter: { email: { $regex: /^edcilinsilva@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Post-operative care" } }
  }},
  // Edel Henry (XH_31945)
  { updateOne: {
      filter: { email: { $regex: /^edelhenry01@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // edwina Egharevba (XH_11658)
  { updateOne: {
      filter: { email: { $regex: /^edwinaegharevba26@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Manual handling" } }
  }},
  // Elaine Donlon (XH_30513)
  { updateOne: {
      filter: { email: { $regex: /^donlonelaine2@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Elena Arabela Saraj (XH_0602)
  { updateOne: {
      filter: { email: { $regex: /^e_arabela@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Elizabeth Barrett (XH_24928)
  { updateOne: {
      filter: { email: { $regex: /^Lizzybarrett15@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Ellen ODriscoll (XH_31501)
  { updateOne: {
      filter: { email: { $regex: /^ellenodriscoll7@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Emely Makumire (XH_32575)
  { updateOne: {
      filter: { email: { $regex: /^makumire1986@gmail.com$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Mental health" } }
  }},
  // Emer Mary MC Laughlin (XH_14585)
  { updateOne: {
      filter: { email: { $regex: /^emermclaughlin1@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Emma Lannon (XH_31695)
  { updateOne: {
      filter: { email: { $regex: /^emma.lannon03@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Manual handling" } }
  }},
  // Emma Sullivan ()
  { updateOne: {
      filter: { email: { $regex: /^emsullivan26@hotmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Palliative care" } }
  }},
  // Emmanuel Odachi (XH_30274)
  { updateOne: {
      filter: { email: { $regex: /^emmanuelodachi@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Emmanuel Osei (XH_32687)
  { updateOne: {
      filter: { email: { $regex: /^nanaseitims@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Special observation" } }
  }},
  // Enere Oboh ()
  { updateOne: {
      filter: { email: { $regex: /^enereboh@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Erdine Reselosa (XH_30135)
  { updateOne: {
      filter: { email: { $regex: /^reselosaerdine@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Eric Baldwin (XH_0031)
  { updateOne: {
      filter: { email: { $regex: /^ericbaldwin9@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Erika Gogo (XH_10283)
  { updateOne: {
      filter: { email: { $regex: /^erika.gogo@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Post-operative care" } }
  }},
  // Ese Edobor (XH_31979)
  { updateOne: {
      filter: { email: { $regex: /^eseaganbi2@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Esther Taiwo ()
  { updateOne: {
      filter: { email: { $regex: /^esthertaiwo81@yahoo.ie$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Ethel Chikwekwete (XH_25846)
  { updateOne: {
      filter: { email: { $regex: /^ethiechik@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Eucharia Chikaodinaka Madueke (XH_32914)
  { updateOne: {
      filter: { email: { $regex: /^ukosigwe@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Mental health" } }
  }},
  // Eufemia Torcuator (XH_7700)
  { updateOne: {
      filter: { email: { $regex: /^eufemia.torcuator@yahoo.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Residential care" } }
  }},
  // Eunice Ogbonna (XH_22070)
  { updateOne: {
      filter: { email: { $regex: /^eeogbonna@yahoo.com$/i } },
      update: { $set: { keyword1: "PEG feeding", keyword2: "Assisted feeding" } }
  }},
  // Eva Misca-Darloci (XH_2093)
  { updateOne: {
      filter: { email: { $regex: /^eva_darloci@yahoo.ie$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Evelin Sathiyaseelan (XH_18840)
  { updateOne: {
      filter: { email: { $regex: /^evelynpreetha@yahoo.co.uk$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Home care" } }
  }},
  // Evelyn Tahongai (XH_32597)
  { updateOne: {
      filter: { email: { $regex: /^evetahongana@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Faith Evbuomwan (XH_16188)
  { updateOne: {
      filter: { email: { $regex: /^evbuomwanf@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Faith Foya (XH_7523)
  { updateOne: {
      filter: { email: { $regex: /^foyafaith@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Manual handling" } }
  }},
  // Farai Nyamuda (XH_3757)
  { updateOne: {
      filter: { email: { $regex: /^farnyamuda@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Fidelma White (XH_0989)
  { updateOne: {
      filter: { email: { $regex: /^fidelmakennerney293_a9s@indeedemail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Flored Ulili (XH_31109)
  { updateOne: {
      filter: { email: { $regex: /^flored3ulili@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Francis Christian Oyangoren Alegarbes (XH_30059)
  { updateOne: {
      filter: { email: { $regex: /^francischristianalegarbes0629@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Francisca Onyeama (XH_31847)
  { updateOne: {
      filter: { email: { $regex: /^nkynna@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Gabriela Warszawska (XH_2119)
  { updateOne: {
      filter: { email: { $regex: /^gabriela.warszawska@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Ganapriya k Virippil (XH_31904)
  { updateOne: {
      filter: { email: { $regex: /^kvganapriya99@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Gaolathe Magola ()
  { updateOne: {
      filter: { email: { $regex: /^magolagaolathe@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Gayathri Deventhiran (XH_0761)
  { updateOne: {
      filter: { email: { $regex: /^Gayud3118@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Gcinile Curtis (XH_30422)
  { updateOne: {
      filter: { email: { $regex: /^gcinilecurtis@gmail.com$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Manual handling" } }
  }},
  // George P Thomas (XH_32395)
  { updateOne: {
      filter: { email: { $regex: /^georgepaulthomas@hotmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // George Thomas ()
  { updateOne: {
      filter: { email: { $regex: /^georgetchamakalayil@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Palliative care" } }
  }},
  // Georgina Ogechukwu Obodoechi (XH_10260)
  { updateOne: {
      filter: { email: { $regex: /^ebube_mesoma@yahoo.co.uk$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Intellectual disability" } }
  }},
  // Gerald Aniakor (XH_0888)
  { updateOne: {
      filter: { email: { $regex: /^geraldelvis.01@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Manual handling" } }
  }},
  // Gian Galapon (XH_30736)
  { updateOne: {
      filter: { email: { $regex: /^dupaxb0y@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Giby Gopi (XH_32540)
  { updateOne: {
      filter: { email: { $regex: /^gibygopi89@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Residential care" } }
  }},
  // Gloria Lugemba (XH_1964)
  { updateOne: {
      filter: { email: { $regex: /^glorialugemba@gmail.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Chronic illness management" } }
  }},
  // Glory Familola ()
  { updateOne: {
      filter: { email: { $regex: /^gofamilola@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Golda Thomas (XH_32045)
  { updateOne: {
      filter: { email: { $regex: /^goldaditto@yahoo.ie$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Gopika Santhi Hari (XH_30751)
  { updateOne: {
      filter: { email: { $regex: /^gopikasanthihari94@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Grace Chikomba (XH_32387)
  { updateOne: {
      filter: { email: { $regex: /^gracechikomba1@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Grace sabili (XH_4884)
  { updateOne: {
      filter: { email: { $regex: /^gracesabili@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Greeshma Sunny (XH_32411)
  { updateOne: {
      filter: { email: { $regex: /^greeshmajmj@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Hamza Sadiq (XH_31993)
  { updateOne: {
      filter: { email: { $regex: /^sadiqhamza688@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Residential care" } }
  }},
  // Hannah Gorman ()
  { updateOne: {
      filter: { email: { $regex: /^gormanh02@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Manual handling" } }
  }},
  // Haritha Banarji (XH_30885)
  { updateOne: {
      filter: { email: { $regex: /^harithabanarji95@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Harvinder Sharma (XH_4478)
  { updateOne: {
      filter: { email: { $regex: /^taksh.mannat@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Special observation" } }
  }},
  // Hayley Walsh (XH_18826)
  { updateOne: {
      filter: { email: { $regex: /^hayleycwalsh@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Hazvinei Kapenzi (XH_14518)
  { updateOne: {
      filter: { email: { $regex: /^hazviekape@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Hazvinei Mvumi (XH_32291)
  { updateOne: {
      filter: { email: { $regex: /^hazvineizelda@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Intellectual disability" } }
  }},
  // Hetty Messitt ()
  { updateOne: {
      filter: { email: { $regex: /^hettymae@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Hima Martin (XH_31590)
  { updateOne: {
      filter: { email: { $regex: /^himamartin1996@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Acute hospital wards" } }
  }},
  // Hlonipani Chivengwa (XH_32534)
  { updateOne: {
      filter: { email: { $regex: /^hmoyochivengwa@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Houda Mhanni ()
  { updateOne: {
      filter: { email: { $regex: /^houdamhanni43@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Hugh Fleming (XH_30000)
  { updateOne: {
      filter: { email: { $regex: /^hughpfleming@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Irish Mary (XH_32327)
  { updateOne: {
      filter: { email: { $regex: /^irishirene1998@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Isaline Collin (XH_20614)
  { updateOne: {
      filter: { email: { $regex: /^isaline.collin123@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Iwona Bartnikiewicz ()
  { updateOne: {
      filter: { email: { $regex: /^bartnikiewicziwona@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Chronic illness management" } }
  }},
  // Jacqueline Inkumsah ()
  { updateOne: {
      filter: { email: { $regex: /^inkumsahjacqueline@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Jan Tharakan (XH_10960)
  { updateOne: {
      filter: { email: { $regex: /^janmariamt@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Manual handling" } }
  }},
  // Jane Chambers ()
  { updateOne: {
      filter: { email: { $regex: /^janew.0099@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Assisted feeding" } }
  }},
  // Jane Gathoni Wambugu (XH_32273)
  { updateOne: {
      filter: { email: { $regex: /^gjaneus@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Jane Marin Sam (XH_8845)
  { updateOne: {
      filter: { email: { $regex: /^janemarinsam@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Jaseena P Kkudy (XH_3504)
  { updateOne: {
      filter: { email: { $regex: /^jaseenamuhammed1995@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Chronic illness management" } }
  }},
  // Jasmi Shanavas (XH_30313)
  { updateOne: {
      filter: { email: { $regex: /^krishnadascsac@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Jean Nguekeng (XH_0481)
  { updateOne: {
      filter: { email: { $regex: /^j.kenfackdidier@gmail.com$/i } },
      update: { $set: { keyword1: "Home care", keyword2: "Residential care" } }
  }},
  // Jebarani Yesudhas (XH_32753)
  { updateOne: {
      filter: { email: { $regex: /^jebaemmanz@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Jeeva Paulose (XH_32250)
  { updateOne: {
      filter: { email: { $regex: /^jeevapaulose8@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Jeleel Olufemi Gbadamosi (XH_32360)
  { updateOne: {
      filter: { email: { $regex: /^yinka375@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Jennifer Orani (XH_32019)
  { updateOne: {
      filter: { email: { $regex: /^jenniferorani@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Jennifer Reyes (XH_32669)
  { updateOne: {
      filter: { email: { $regex: /^jenpaterno14@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Residential care" } }
  }},
  // Jessy Mukombachoto (XH_1422)
  { updateOne: {
      filter: { email: { $regex: /^jessymukombachoto@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Jester Zhou (XH_30120)
  { updateOne: {
      filter: { email: { $regex: /^zhoujester@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Jherlyn Perile (XH_3313)
  { updateOne: {
      filter: { email: { $regex: /^jherlynpbolado@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Jhon Baldoz (XH_25898)
  { updateOne: {
      filter: { email: { $regex: /^artista.baldoz@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Jijo Jerlin (XH_0038)
  { updateOne: {
      filter: { email: { $regex: /^jijojerlin@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Jiju Jose ()
  { updateOne: {
      filter: { email: { $regex: /^jijupph@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Jimsy Joy (XH_33282)
  { updateOne: {
      filter: { email: { $regex: /^jimsyjoy1993@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Residential care" } }
  }},
  // Jinu Kurian ()
  { updateOne: {
      filter: { email: { $regex: /^mariyajinu2011@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Jisby Jose (XH_9254)
  { updateOne: {
      filter: { email: { $regex: /^jisbyjose36@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Jismol Alias (XH_11529)
  { updateOne: {
      filter: { email: { $regex: /^jismolalias96@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Jisni Thomas (XH_32650)
  { updateOne: {
      filter: { email: { $regex: /^deepulaljisni@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Jisrani Sunny ()
  { updateOne: {
      filter: { email: { $regex: /^sunnyjisrani@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Joan Clerkin (XH_7418)
  { updateOne: {
      filter: { email: { $regex: /^joanclerkin8@gmail.com$/i } },
      update: { $set: { keyword1: "Palliative care", keyword2: "Post-operative care" } }
  }},
  // Joanne Burns (XH_31461)
  { updateOne: {
      filter: { email: { $regex: /^joanneburns240@gmail.com$/i } },
      update: { $set: { keyword1: "Palliative care", keyword2: "Assisted feeding" } }
  }},
  // Joe Abraham (XH_19438)
  { updateOne: {
      filter: { email: { $regex: /^joeannvarghese35@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Johnson Ugwu (XH_32120)
  { updateOne: {
      filter: { email: { $regex: /^ositajohnson1990@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Joicy Jose (XH_30173)
  { updateOne: {
      filter: { email: { $regex: /^joicyjose2016@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Jolly John (XH_32935)
  { updateOne: {
      filter: { email: { $regex: /^jollyj2322@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Jomol Pulivelil (XH_7071)
  { updateOne: {
      filter: { email: { $regex: /^jomol_06@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Jonathan Salvacion (XH_32841)
  { updateOne: {
      filter: { email: { $regex: /^athan.liam@yahoo.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Home care" } }
  }},
  // Josephine Moloney (XH_30980)
  { updateOne: {
      filter: { email: { $regex: /^josephinemoloney1@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Joshua Okoro (XH_31803)
  { updateOne: {
      filter: { email: { $regex: /^joshuaokoro8@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Joy Holonga (XH_0734)
  { updateOne: {
      filter: { email: { $regex: /^holongaj@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Judith Ngureta (XH_32361)
  { updateOne: {
      filter: { email: { $regex: /^judyngureta@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Juliana Osazuwa (XH_19838)
  { updateOne: {
      filter: { email: { $regex: /^julianeosazuwa@yahoo.it$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Juliet Kennedy (XH_30126)
  { updateOne: {
      filter: { email: { $regex: /^juliekennedy2000@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Assisted feeding" } }
  }},
  // Juliet Tsaurai (XH_26148)
  { updateOne: {
      filter: { email: { $regex: /^jtsaurai@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Karen Godfrey (XH_1875)
  { updateOne: {
      filter: { email: { $regex: /^karengodfrey150@ymail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Karen Turnbull (XH_16030)
  { updateOne: {
      filter: { email: { $regex: /^karenturnbull18@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Intellectual disability" } }
  }},
  // Karlin Crombie (XH_2134)
  { updateOne: {
      filter: { email: { $regex: /^karlincrombie1992@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Kate Stanley (XH_33097)
  { updateOne: {
      filter: { email: { $regex: /^kateannestan@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Kathiravan Soundhara Pandian (XH_30290)
  { updateOne: {
      filter: { email: { $regex: /^kathirbeta@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Kenechukwu Egbochukwu (XH_33315)
  { updateOne: {
      filter: { email: { $regex: /^charliekene60@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Residential care" } }
  }},
  // Kezia Jacob (XH_32064)
  { updateOne: {
      filter: { email: { $regex: /^keziajacob25@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Kingsley Amobi (XH_30215)
  { updateOne: {
      filter: { email: { $regex: /^kicia77@hotmail.co.uk$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Kochurani Kalapparambath Varghese (XH_9983)
  { updateOne: {
      filter: { email: { $regex: /^kvkochurani@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Krishnendu Santhosh (XH_23412)
  { updateOne: {
      filter: { email: { $regex: /^krishnendusanthosh99@gmail.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Elderly care" } }
  }},
  // Krzysztof Borkowski (XH_32573)
  { updateOne: {
      filter: { email: { $regex: /^borekk145@o2.pl$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Kusuma Kaenmuang (XH_33055)
  { updateOne: {
      filter: { email: { $regex: /^koosoomah@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Laline Martins (XH_31110)
  { updateOne: {
      filter: { email: { $regex: /^l.ramosmartins132@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Laneesh Sasi (XH_29982)
  { updateOne: {
      filter: { email: { $regex: /^laneesh047@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Palliative care" } }
  }},
  // Larisa Candalea (XH_0040)
  { updateOne: {
      filter: { email: { $regex: /^larisacandalea@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Laura Jones (XH_32821)
  { updateOne: {
      filter: { email: { $regex: /^laurajonesty13@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Chronic illness management" } }
  }},
  // Leema Thomas (XH_16540)
  { updateOne: {
      filter: { email: { $regex: /^leemaaji86@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Leena T Thomas (XH_0512)
  { updateOne: {
      filter: { email: { $regex: /^leenamanu100@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Leena Thomas (XH_23745)
  { updateOne: {
      filter: { email: { $regex: /^leenat98@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Letbee Letbee Zishiri (XH_10029)
  { updateOne: {
      filter: { email: { $regex: /^letbeezishiri@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Letwin Mutambashora (XH_32509)
  { updateOne: {
      filter: { email: { $regex: /^tarisaimutambashora@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Special observation" } }
  }},
  // Liju  Philip Philipose (XH_2124)
  { updateOne: {
      filter: { email: { $regex: /^liju.philip@ymail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Lilly Asare ()
  { updateOne: {
      filter: { email: { $regex: /^asarelilly@gmail.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Post-operative care" } }
  }},
  // Lincy Thankachan (XH_31023)
  { updateOne: {
      filter: { email: { $regex: /^jobyaaronjoby@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Linda Ann Mahembe (XH_1883)
  { updateOne: {
      filter: { email: { $regex: /^mahembelindaann@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Linnu Thomas (XH_32714)
  { updateOne: {
      filter: { email: { $regex: /^tlinnu92@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Linta George (XH_31772)
  { updateOne: {
      filter: { email: { $regex: /^lintajoby350@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Little Rose Thomas (XH_13295)
  { updateOne: {
      filter: { email: { $regex: /^leemarosemph2015@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Emergency department" } }
  }},
  // Liviza Victor (XH_30793)
  { updateOne: {
      filter: { email: { $regex: /^livizavictor9878@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Special observation" } }
  }},
  // Liz kelly (XH_21724)
  { updateOne: {
      filter: { email: { $regex: /^lizkellyster@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Chronic illness management" } }
  }},
  // Lorraine Ryan ()
  { updateOne: {
      filter: { email: { $regex: /^92lorrainemannion@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Intellectual disability" } }
  }},
  // Lovelyn Akolade (XH_7276)
  { updateOne: {
      filter: { email: { $regex: /^chirpyluvlyn007@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Luciana Silivestru ()
  { updateOne: {
      filter: { email: { $regex: /^silivestru.maria.luciana@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Madelyn Manicat (XH_32710)
  { updateOne: {
      filter: { email: { $regex: /^mhadzmanicat@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Manju Joseph (XH_32175)
  { updateOne: {
      filter: { email: { $regex: /^manjujoseph032@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Post-operative care" } }
  }},
  // Maria Alosious (XH_32069)
  { updateOne: {
      filter: { email: { $regex: /^mariarosealosious966@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Maria Rhoda Abdollah (XH_19620)
  { updateOne: {
      filter: { email: { $regex: /^mariarabdollah@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Maria Varghese (XH_32403)
  { updateOne: {
      filter: { email: { $regex: /^maria.varghese11@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Elderly care" } }
  }},
  // Marita Ubasonye (XH_31391)
  { updateOne: {
      filter: { email: { $regex: /^mubasonye@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Maritess Dignos (XH_32608)
  { updateOne: {
      filter: { email: { $regex: /^tesdignos@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Mariya Jose ()
  { updateOne: {
      filter: { email: { $regex: /^annamariyarose95@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Emergency department" } }
  }},
  // Mariya Souriar (XH_32834)
  { updateOne: {
      filter: { email: { $regex: /^mariyacs98@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Residential care" } }
  }},
  // Mark Lyons (XH_26765)
  { updateOne: {
      filter: { email: { $regex: /^lyonsmark1999@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Marnille Bustos (XH_30411)
  { updateOne: {
      filter: { email: { $regex: /^marnillebustos@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Special observation" } }
  }},
  // Martha Killeen (XH_8125)
  { updateOne: {
      filter: { email: { $regex: /^Martha.Killeen@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Mary Jacob (XH_30138)
  { updateOne: {
      filter: { email: { $regex: /^sonamary442@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Post-operative care" } }
  }},
  // Mary jo Hickey Merrins (XH_16715)
  { updateOne: {
      filter: { email: { $regex: /^Mjmerrins@icloud.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Mary McNamara ()
  { updateOne: {
      filter: { email: { $regex: /^m.j.mcn@hotmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Mary Milanzi (XH_10729)
  { updateOne: {
      filter: { email: { $regex: /^ndaipenimary@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Mary Smith (XH_24299)
  { updateOne: {
      filter: { email: { $regex: /^marysmith404@gmail.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Manual handling" } }
  }},
  // Marygin Panton (XH_32071)
  { updateOne: {
      filter: { email: { $regex: /^pantonjhing@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Matankiso Mothobi ()
  { updateOne: {
      filter: { email: { $regex: /^matankisomothobi225@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Meenu Baby ()
  { updateOne: {
      filter: { email: { $regex: /^meenubaby16@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Meenu Thekkemanankathadathil (XH_30238)
  { updateOne: {
      filter: { email: { $regex: /^meenujacob2020@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Megan Foley (XH_27260)
  { updateOne: {
      filter: { email: { $regex: /^foleymegan13@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Manual handling" } }
  }},
  // Melanie Maloney (XH_5779)
  { updateOne: {
      filter: { email: { $regex: /^melaniemaloney@hotmail.co.uk$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Chronic illness management" } }
  }},
  // Melbin Thomas ()
  { updateOne: {
      filter: { email: { $regex: /^melbinthomas078@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Residential care" } }
  }},
  // Mercy Musasa (XH_5709)
  { updateOne: {
      filter: { email: { $regex: /^sophiecondon10@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // merlin abraham ()
  { updateOne: {
      filter: { email: { $regex: /^merlintreesaka@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Mica Cabanag (XH_11685)
  { updateOne: {
      filter: { email: { $regex: /^micadanelcabanag@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Residential care" } }
  }},
  // Michelle Allison (O' CONNOR) ()
  { updateOne: {
      filter: { email: { $regex: /^milly72uk@yahoo.co.uk$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Mental health" } }
  }},
  // Michelle Mcglone (XH_1933)
  { updateOne: {
      filter: { email: { $regex: /^megamum.michelle@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Michelle Mupota (XH_1241)
  { updateOne: {
      filter: { email: { $regex: /^michellemupota36@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Milarose Mae Cabarce (XH_20187)
  { updateOne: {
      filter: { email: { $regex: /^milrose88@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Special observation" } }
  }},
  // Milka Thomas ()
  { updateOne: {
      filter: { email: { $regex: /^milkaansa4all@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Minenhle Moyo (XH_30030)
  { updateOne: {
      filter: { email: { $regex: /^moyominenhle@icloud.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Mini Mathew (XH_29781)
  { updateOne: {
      filter: { email: { $regex: /^minimdublin@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Special observation" } }
  }},
  // Minu Tom (XH_29754)
  { updateOne: {
      filter: { email: { $regex: /^minumary90@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Miriam Nwaeme (XH_30413)
  { updateOne: {
      filter: { email: { $regex: /^nwaemenkem@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Residential care" } }
  }},
  // Mogue McGovern (XH_31334)
  { updateOne: {
      filter: { email: { $regex: /^mogue.mcgovern@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Moira Ryan (XH_6140)
  { updateOne: {
      filter: { email: { $regex: /^moiralryan@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Molgy Antony (XH_33047)
  { updateOne: {
      filter: { email: { $regex: /^molgyantony11015@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Residential care" } }
  }},
  // Monstancia Machonisa (XH_30407)
  { updateOne: {
      filter: { email: { $regex: /^mmonstancia89@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Chronic illness management" } }
  }},
  // Moya Howse (XH_29997)
  { updateOne: {
      filter: { email: { $regex: /^moyahowse98@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Muhammed Chakkittammel ()
  { updateOne: {
      filter: { email: { $regex: /^naseefmanu@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Post-operative care" } }
  }},
  // Mzia Kopaliani ()
  { updateOne: {
      filter: { email: { $regex: /^mziako84@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Naomi Allotey (XH_31041)
  { updateOne: {
      filter: { email: { $regex: /^naomiallotey@outlook.com$/i } },
      update: { $set: { keyword1: "Palliative care", keyword2: "Mental health" } }
  }},
  // Nataschamarie Houston ()
  { updateOne: {
      filter: { email: { $regex: /^l00104538@atu.ie$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Mental health" } }
  }},
  // Navya Abraham (XH_31214)
  { updateOne: {
      filter: { email: { $regex: /^navyaabraham6@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Ndadyei Gudza (XH_32159)
  { updateOne: {
      filter: { email: { $regex: /^ndadyei@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Neenu George (XH_0331)
  { updateOne: {
      filter: { email: { $regex: /^neenunicky91@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Neethu Benjamin (XH_32343)
  { updateOne: {
      filter: { email: { $regex: /^neethualice87@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Niall Brady (XH_32238)
  { updateOne: {
      filter: { email: { $regex: /^niallbrady9991@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Chronic illness management" } }
  }},
  // Niamh Lawlor (XH_26841)
  { updateOne: {
      filter: { email: { $regex: /^niamhlawlor12@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Manual handling" } }
  }},
  // Nihitha Limistan (XH_32972)
  { updateOne: {
      filter: { email: { $regex: /^nihithalimston3108@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Nikki Chacko (XH_21445)
  { updateOne: {
      filter: { email: { $regex: /^nikki.chacko666@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Nisha Alex (XH_23039)
  { updateOne: {
      filter: { email: { $regex: /^alexnisha1989@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Nisha Thankachen (XH_28620)
  { updateOne: {
      filter: { email: { $regex: /^nishabinuktr@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Nithiya Simon ()
  { updateOne: {
      filter: { email: { $regex: /^nithiyasimon67@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Post-operative care" } }
  }},
  // Njideka Obidinnu (XH_32156)
  { updateOne: {
      filter: { email: { $regex: /^nnuobidinnu4real@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Noelle Brennan (XH_6594)
  { updateOne: {
      filter: { email: { $regex: /^noellebrennanc@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Noirin Regan (XH_32902)
  { updateOne: {
      filter: { email: { $regex: /^noirinregan@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Nollaig Broe ()
  { updateOne: {
      filter: { email: { $regex: /^nbroe12@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Nora Maduabuchi ()
  { updateOne: {
      filter: { email: { $regex: /^norachisom90@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Manual handling" } }
  }},
  // Noreen Ngirazi (XH_30931)
  { updateOne: {
      filter: { email: { $regex: /^noreenngirazi22@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Noreen Sadiq (XH_32049)
  { updateOne: {
      filter: { email: { $regex: /^noreen.sadiq1512@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Ntobeko Moyo ()
  { updateOne: {
      filter: { email: { $regex: /^ntoemoyo13@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Nyasha Chirozva (XH_21873)
  { updateOne: {
      filter: { email: { $regex: /^nyashaachirozva@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Odion Ugbiyobo (XH_11680)
  { updateOne: {
      filter: { email: { $regex: /^odionpauline@gmail.com$/i } },
      update: { $set: { keyword1: "Home care", keyword2: "Chronic illness management" } }
  }},
  // Ogochukwu Ajuluchukwu (XH_32152)
  { updateOne: {
      filter: { email: { $regex: /^ajuluchukwuogo@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Chronic illness management" } }
  }},
  // Ogochukwu steve Tamuno (XH_32684)
  { updateOne: {
      filter: { email: { $regex: /^destinymbonu2015@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Post-operative care" } }
  }},
  // Okioghene Odiku (XH_30385)
  { updateOne: {
      filter: { email: { $regex: /^joelodiku@yahoo.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Emergency department" } }
  }},
  // Olivia O Meara (XH_31528)
  { updateOne: {
      filter: { email: { $regex: /^oliviaomeara1@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Manual handling" } }
  }},
  // Olivia Proskurnya (XH_30212)
  { updateOne: {
      filter: { email: { $regex: /^olivia0944@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Olufunmilayo Jenmi ()
  { updateOne: {
      filter: { email: { $regex: /^milayosalami@yahoo.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Olusola Abioye (XH_30900)
  { updateOne: {
      filter: { email: { $regex: /^olusolasundayabioye@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Olusola Omowaye (XH_31127)
  { updateOne: {
      filter: { email: { $regex: /^omowayeolusola@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Intellectual disability" } }
  }},
  // Omoyemie Otabor ()
  { updateOne: {
      filter: { email: { $regex: /^omoyemieotabor@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Onthatile Gopolang ()
  { updateOne: {
      filter: { email: { $regex: /^ontygopola@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Manual handling" } }
  }},
  // Oonelumhe Usman ()
  { updateOne: {
      filter: { email: { $regex: /^adeleyeoone@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Orla Killeen (XH_25347)
  { updateOne: {
      filter: { email: { $regex: /^killeen.orla@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Manual handling" } }
  }},
  // Pamela Maidza (XH_30987)
  { updateOne: {
      filter: { email: { $regex: /^pamelamaidza1@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Pardon Chinyoka (XH_31743)
  { updateOne: {
      filter: { email: { $regex: /^chinyokap@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Pascaline Obi (XH_32976)
  { updateOne: {
      filter: { email: { $regex: /^obipascaline4@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Patricia Owens (XH_2821)
  { updateOne: {
      filter: { email: { $regex: /^patriciaowens1998@hotmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Paulina Rudzinska (XH_31010)
  { updateOne: {
      filter: { email: { $regex: /^paulina.rudzinska@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Peace Okafor (XH_1100)
  { updateOne: {
      filter: { email: { $regex: /^peacendii@yahoo.co.uk$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Home care" } }
  }},
  // Pennina Kwanele (XH_1003)
  { updateOne: {
      filter: { email: { $regex: /^penninakmoyo@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Petraki Cobo (XH_30157)
  { updateOne: {
      filter: { email: { $regex: /^petrakitsobo@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Plaxcedes Jeche (XH_12609)
  { updateOne: {
      filter: { email: { $regex: /^plaxyjeche@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Plaxedes Chinganga (XH_31390)
  { updateOne: {
      filter: { email: { $regex: /^chingangap@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Manual handling" } }
  }},
  // Portia Muchenje (XH_31387)
  { updateOne: {
      filter: { email: { $regex: /^portiamuchenje249@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Manual handling" } }
  }},
  // Pran Raj Mundadan (XH_0073)
  { updateOne: {
      filter: { email: { $regex: /^pranmundadan@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Emergency department" } }
  }},
  // Praveen Eralamkalayil (XH_15485)
  { updateOne: {
      filter: { email: { $regex: /^praveenek88@gmail.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Post-operative care" } }
  }},
  // Praveen Jose (XH_30015)
  { updateOne: {
      filter: { email: { $regex: /^praveenkudililjose@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Princy Jose ()
  { updateOne: {
      filter: { email: { $regex: /^ponnuabin18@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Princy Praveen (XH_31490)
  { updateOne: {
      filter: { email: { $regex: /^princy902@gmail.com$/i } },
      update: { $set: { keyword1: "Palliative care", keyword2: "Acute hospital wards" } }
  }},
  // Rachel Barrett (XH_32105)
  { updateOne: {
      filter: { email: { $regex: /^rachelbarrett7@yahoo.ie$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Rachel Chawatama (XH_17835)
  { updateOne: {
      filter: { email: { $regex: /^chawatamarachel@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Rachel Connolly (XH_31484)
  { updateOne: {
      filter: { email: { $regex: /^rachelconnolly27@hotmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Chronic illness management" } }
  }},
  // Rachel Houlihan (XH_29952)
  { updateOne: {
      filter: { email: { $regex: /^rachelmaryhoulihan@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Rachel Mather (XH_32695)
  { updateOne: {
      filter: { email: { $regex: /^rachelmather1998@outlook.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Assisted feeding" } }
  }},
  // Rachel Tshuma (XH_31722)
  { updateOne: {
      filter: { email: { $regex: /^rachelpodatshuma@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Raimonda Vasilenkiene (XH_31714)
  { updateOne: {
      filter: { email: { $regex: /^raimusia@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Chronic illness management" } }
  }},
  // Rakhi kuttan (XH_7553)
  { updateOne: {
      filter: { email: { $regex: /^rakhikuttan4@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Post-operative care" } }
  }},
  // Rani George (XH_32253)
  { updateOne: {
      filter: { email: { $regex: /^ranigeorge1994@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Raymond Katsidzira (XH_32308)
  { updateOne: {
      filter: { email: { $regex: /^tawahray@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Razack Abdulrahiman ()
  { updateOne: {
      filter: { email: { $regex: /^razackmoh@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Rebecca North (XH_32671)
  { updateOne: {
      filter: { email: { $regex: /^r.north90@outlook.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Remya Baby (XH_30595)
  { updateOne: {
      filter: { email: { $regex: /^remyababy2811@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Remya K Oommen (XH_5991)
  { updateOne: {
      filter: { email: { $regex: /^remyabijoy2014@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Renu Bala (XH_32505)
  { updateOne: {
      filter: { email: { $regex: /^pr.ashiq@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Richelle O'Neill ()
  { updateOne: {
      filter: { email: { $regex: /^shellyo85@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Rini Rajan (XH_25810)
  { updateOne: {
      filter: { email: { $regex: /^rajanrini04@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Riny Raju ()
  { updateOne: {
      filter: { email: { $regex: /^rinyraju07@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Special observation" } }
  }},
  // Rishi Raj (XH_20149)
  { updateOne: {
      filter: { email: { $regex: /^rishikraj4868@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Rita Noseviciute (XH_12551)
  { updateOne: {
      filter: { email: { $regex: /^ritanoseviciute@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Ritchie Velasco (XH_0929)
  { updateOne: {
      filter: { email: { $regex: /^romeoryan_velasco@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Rochelle Siao (XH_2422)
  { updateOne: {
      filter: { email: { $regex: /^chelle7166@yahoo.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Residential care" } }
  }},
  // Rosalind Halpin (XH_30748)
  { updateOne: {
      filter: { email: { $regex: /^hollyh19@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Rosemary Healy (XH_29969)
  { updateOne: {
      filter: { email: { $regex: /^rosiehealy7@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Rosemary Varghese (XH_19896)
  { updateOne: {
      filter: { email: { $regex: /^rosemaryvarghese94@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Roshni Joseph ()
  { updateOne: {
      filter: { email: { $regex: /^roshnijoseph3april@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Ruby Jacob (XH_31481)
  { updateOne: {
      filter: { email: { $regex: /^ruby.jacob018@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Ruqiya Pathan (XH_19814)
  { updateOne: {
      filter: { email: { $regex: /^ruqiyapathan20@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Chronic illness management" } }
  }},
  // Sabitha Prabhakaran ()
  { updateOne: {
      filter: { email: { $regex: /^sabimol.p@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Sabrina Byrne (XH_30874)
  { updateOne: {
      filter: { email: { $regex: /^sabrina19577@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Sajjana Koovely Chacko (XH_0780)
  { updateOne: {
      filter: { email: { $regex: /^sajjanakoovelychacko5_sgk@indeedemail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Sajo Jose (XH_0685)
  { updateOne: {
      filter: { email: { $regex: /^sajo1234@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Salini Joseph (XH_21449)
  { updateOne: {
      filter: { email: { $regex: /^shalzaj@yahoo.co.in$/i } },
      update: { $set: { keyword1: "Assisted feeding", keyword2: "Chronic illness management" } }
  }},
  // Salja Kochappan (XH_32513)
  { updateOne: {
      filter: { email: { $regex: /^saljabaiju7@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Samantha Makiwa (XH_30072)
  { updateOne: {
      filter: { email: { $regex: /^samanthamakiwa@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Manual handling" } }
  }},
  // Samitha Ramannashetty (XH_30123)
  { updateOne: {
      filter: { email: { $regex: /^samithaksnc@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Residential care" } }
  }},
  // Sampson Aduri (XH_32088)
  { updateOne: {
      filter: { email: { $regex: /^adurisa12@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Sandeep Kalathithara Saseendran (XH_7267)
  { updateOne: {
      filter: { email: { $regex: /^sandeepakaunni@icloud.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Sandra Joshy (XH_32439)
  { updateOne: {
      filter: { email: { $regex: /^sandrakjoshy@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Sandra Ward (XH_2306)
  { updateOne: {
      filter: { email: { $regex: /^sandra.ward2024@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Sandrine Mbenza ()
  { updateOne: {
      filter: { email: { $regex: /^smbenzaplc@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Home care" } }
  }},
  // Sanitha Sebastian (XH_19216)
  { updateOne: {
      filter: { email: { $regex: /^sebastiansanitha@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Emergency department" } }
  }},
  // Sany Antony ()
  { updateOne: {
      filter: { email: { $regex: /^sanyantony1988@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Post-operative care" } }
  }},
  // Sara Almeida (XH_31165)
  { updateOne: {
      filter: { email: { $regex: /^sarasilvacastro@gmail.com$/i } },
      update: { $set: { keyword1: "Palliative care", keyword2: "Residential care" } }
  }},
  // Sarah Nassali (XH_30982)
  { updateOne: {
      filter: { email: { $regex: /^nassalira@gmail.com$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Elderly care" } }
  }},
  // Seena John (XH_32676)
  { updateOne: {
      filter: { email: { $regex: /^seenaj20@yahoo.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Seeniamol Reji (XH_32616)
  { updateOne: {
      filter: { email: { $regex: /^seeniamolreji264@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Selina Mulkeen (XH_32595)
  { updateOne: {
      filter: { email: { $regex: /^selinamulkeen5@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Shadrack Enebeli (XH_32338)
  { updateOne: {
      filter: { email: { $regex: /^enebelishadrack12@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Assisted feeding" } }
  }},
  // Shafqat Ali ()
  { updateOne: {
      filter: { email: { $regex: /^shafqat342@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Elderly care" } }
  }},
  // Shallet Manoj ()
  { updateOne: {
      filter: { email: { $regex: /^shalletmanoj2005@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Shamiso Chakara (XH_30289)
  { updateOne: {
      filter: { email: { $regex: /^shammychak@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Elderly care" } }
  }},
  // Shannon Tighe (XH_28759)
  { updateOne: {
      filter: { email: { $regex: /^shannon.tighe12@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Intellectual disability" } }
  }},
  // Shauna O'Brien (XH_19902)
  { updateOne: {
      filter: { email: { $regex: /^shauna97obrien@gmail.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Chronic illness management" } }
  }},
  // Sheeba Achankunju (XH_30400)
  { updateOne: {
      filter: { email: { $regex: /^sheeba15achu@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Sheeba Varghese (XH_32906)
  { updateOne: {
      filter: { email: { $regex: /^geogisheeba@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Post-operative care" } }
  }},
  // Sheen Thomas (XH_4060)
  { updateOne: {
      filter: { email: { $regex: /^sheenmt1@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Shelter Mugova (XH_30247)
  { updateOne: {
      filter: { email: { $regex: /^smugova21@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Emergency department" } }
  }},
  // Shelton Kwashi (XH_9805)
  { updateOne: {
      filter: { email: { $regex: /^sheltonkwashie@gmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Chronic illness management" } }
  }},
  // Shingai Grace Chiwetu (XH_4247)
  { updateOne: {
      filter: { email: { $regex: /^chiwetushingai@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Shiny Sunny (XH_32837)
  { updateOne: {
      filter: { email: { $regex: /^shinyevajohn@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Shirly Passely (XH_31740)
  { updateOne: {
      filter: { email: { $regex: /^shirlypassely@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Shumirai Mundandi (XH_1807)
  { updateOne: {
      filter: { email: { $regex: /^shumiemundandi@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Manual handling" } }
  }},
  // Sijo Jose ()
  { updateOne: {
      filter: { email: { $regex: /^josesijo338@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Silvija Jakovcic (XH_11212)
  { updateOne: {
      filter: { email: { $regex: /^silvijaposavec8@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Silviu Gusuvate (XH_33101)
  { updateOne: {
      filter: { email: { $regex: /^gusuvateibuprofen@yahoo.ro$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Sinead Feely ()
  { updateOne: {
      filter: { email: { $regex: /^merlintreesaka@gmail.com$/i } },
      update: { $set: { keyword1: "Residential care", keyword2: "Intellectual disability" } }
  }},
  // Sinimol Varghese (XH_6580)
  { updateOne: {
      filter: { email: { $regex: /^sinijinesh8586@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Siobhan Bridget Greevy (XH_2899)
  { updateOne: {
      filter: { email: { $regex: /^greevysiobhan@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Siobhan Holland (XH_32030)
  { updateOne: {
      filter: { email: { $regex: /^siobhanculver@hotmail.com$/i } },
      update: { $set: { keyword1: "Mental health", keyword2: "Intellectual disability" } }
  }},
  // Snehamole Sarala (XH_0882)
  { updateOne: {
      filter: { email: { $regex: /^snehaalappy@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Sofia Al-Shakakheh (XH_30356)
  { updateOne: {
      filter: { email: { $regex: /^Sofia.sofee255@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Sona Peter (XH_32626)
  { updateOne: {
      filter: { email: { $regex: /^sonapeter1989@gmail.com$/i } },
      update: { $set: { keyword1: "Palliative care", keyword2: "Manual handling" } }
  }},
  // Soniya Sunny (XH_26279)
  { updateOne: {
      filter: { email: { $regex: /^soniyasunny987@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Sophia Gallagher (XH_30149)
  { updateOne: {
      filter: { email: { $regex: /^sophiagall8gher@gmail.com$/i } },
      update: { $set: { keyword1: "Intellectual disability", keyword2: "Post-operative care" } }
  }},
  // Sreejith Viswanatha ()
  { updateOne: {
      filter: { email: { $regex: /^sreejithvkurupakku@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Residential care" } }
  }},
  // Sreekanth Krishnan ()
  { updateOne: {
      filter: { email: { $regex: /^sreekanthk168@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Residential care" } }
  }},
  // Sreelakshmi Ayyappan (XH_31133)
  { updateOne: {
      filter: { email: { $regex: /^sreelakshmia81@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Sreelakshmi Haridasan (XH_32530)
  { updateOne: {
      filter: { email: { $regex: /^sreelakshmikh.1390@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Srijina Jose (XH_30499)
  { updateOne: {
      filter: { email: { $regex: /^srijinajose04@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Stacey Mary Touhey (XH_16796)
  { updateOne: {
      filter: { email: { $regex: /^staceytouhey@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Steffy Mery Mathew (XH_16636)
  { updateOne: {
      filter: { email: { $regex: /^merysteffymathew@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Emergency department" } }
  }},
  // Stephanie Doyle (XH_1809)
  { updateOne: {
      filter: { email: { $regex: /^sdoyle1@tcd.ie$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Palliative care" } }
  }},
  // Steve Manuel (XH_2052)
  { updateOne: {
      filter: { email: { $regex: /^streakx666@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Acute hospital wards" } }
  }},
  // Subil Subashu Analil (XH_2009)
  { updateOne: {
      filter: { email: { $regex: /^analilsubil@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Sunil Varghese (XH_18737)
  { updateOne: {
      filter: { email: { $regex: /^sunil41b@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Personal care" } }
  }},
  // Sunu Joseph (XH_1986)
  { updateOne: {
      filter: { email: { $regex: /^sunujosephsun@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Svetlana Ozola (XH_19587)
  { updateOne: {
      filter: { email: { $regex: /^svetlana.ozola74@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Swathi Kandula (XH_5119)
  { updateOne: {
      filter: { email: { $regex: /^kandulaswathi7@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Syam Shanmughan (XH_4255)
  { updateOne: {
      filter: { email: { $regex: /^syamps121@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Emergency department" } }
  }},
  // Tamara Tadic ()
  { updateOne: {
      filter: { email: { $regex: /^tamara.vuljanic92@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Tammie De Oca (XH_31619)
  { updateOne: {
      filter: { email: { $regex: /^deoca_t14@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Tanaka Manyere (XH_31855)
  { updateOne: {
      filter: { email: { $regex: /^tanakamanyere8@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Tatendaishe Mudzviti (XH_32190)
  { updateOne: {
      filter: { email: { $regex: /^mudzvititatendaishe@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Tatiana Mira (XH_31556)
  { updateOne: {
      filter: { email: { $regex: /^tfilipamira@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Teena Lal (XH_17952)
  { updateOne: {
      filter: { email: { $regex: /^lal.teena1991@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Residential care" } }
  }},
  // Teighlour Fegan (XH_21047)
  { updateOne: {
      filter: { email: { $regex: /^Teighlourfegan2000@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Tenzin Tsokyi ()
  { updateOne: {
      filter: { email: { $regex: /^tenzintsokyi143@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Manual handling" } }
  }},
  // Thea Fulham (XH_31670)
  { updateOne: {
      filter: { email: { $regex: /^fulhamthea@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Tina Shakari (XH_32590)
  { updateOne: {
      filter: { email: { $regex: /^tinashakari2@gmail.com$/i } },
      update: { $set: { keyword1: "Emergency department", keyword2: "Post-operative care" } }
  }},
  // Tincy Ignatious (XH_31191)
  { updateOne: {
      filter: { email: { $regex: /^tincyignatious@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Tincy Loveju (XH_31107)
  { updateOne: {
      filter: { email: { $regex: /^tincyloveju@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Manual handling" } }
  }},
  // Tinta Varghese (XH_31602)
  { updateOne: {
      filter: { email: { $regex: /^vargheseti677@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Manual handling" } }
  }},
  // Tintomol Joseph (XH_33293)
  { updateOne: {
      filter: { email: { $regex: /^tintomoljoseph21@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
  // Tintu Thomas (XH_32607)
  { updateOne: {
      filter: { email: { $regex: /^tintuanniethomas@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Tolulope Atunrase (XH_6858)
  { updateOne: {
      filter: { email: { $regex: /^toluatunrase@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Trassa Ni Aonghusa (XH_0880)
  { updateOne: {
      filter: { email: { $regex: /^niaonghusatrassa@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Tsitsi kaswa (XH_17221)
  { updateOne: {
      filter: { email: { $regex: /^Kumiretsitsi1@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Special observation" } }
  }},
  // Usha Rajan (XH_31061)
  { updateOne: {
      filter: { email: { $regex: /^ushapradhi@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Vallerie Makuwe (XH_8184)
  { updateOne: {
      filter: { email: { $regex: /^vallerienhamo@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Home care" } }
  }},
  // Vanessa Monthe Djomadje (XH_10654)
  { updateOne: {
      filter: { email: { $regex: /^nounousse38@yahoo.fr$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Manual handling" } }
  }},
  // Velinceafarida Fernandes (XH_1700)
  { updateOne: {
      filter: { email: { $regex: /^velinceafaridafernandes@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Chronic illness management" } }
  }},
  // Vengie Garcia (XH_19354)
  { updateOne: {
      filter: { email: { $regex: /^vng_ghie66@yahoo.ie$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Vera Simo (XH_16947)
  { updateOne: {
      filter: { email: { $regex: /^verasimo2014@yahoo.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Elderly care" } }
  }},
  // Victor Cornel (XH_0084)
  { updateOne: {
      filter: { email: { $regex: /^cornel.victor@yahoo.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Victor Udokwu (XH_2344)
  { updateOne: {
      filter: { email: { $regex: /^abebevictor9@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Home care" } }
  }},
  // Victoria Nwagiriga (XH_30394)
  { updateOne: {
      filter: { email: { $regex: /^vikinwagiriga@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Manual handling" } }
  }},
  // Vinod Gopalan ()
  { updateOne: {
      filter: { email: { $regex: /^gvk901@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Vishnupriya K Vinoba (XH_20223)
  { updateOne: {
      filter: { email: { $regex: /^vishnnupriyavinoba@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Residential care" } }
  }},
  // Vlad Lungu (XH_0060)
  { updateOne: {
      filter: { email: { $regex: /^euvlad57@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Winifred Ani (XH_32393)
  { updateOne: {
      filter: { email: { $regex: /^chinenyeaniblaze@gmail.com$/i } },
      update: { $set: { keyword1: "Post-operative care", keyword2: "Chronic illness management" } }
  }},
  // Yana Vereshchaieva ()
  { updateOne: {
      filter: { email: { $regex: /^vereshchaieva@gmail.com$/i } },
      update: { $set: { keyword1: "Personal care", keyword2: "Chronic illness management" } }
  }},
  // Yevai Bote ()
  { updateOne: {
      filter: { email: { $regex: /^revaihove96@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Emergency department" } }
  }},
  // Yogesh Gopan (XH_32093)
  { updateOne: {
      filter: { email: { $regex: /^yogeshgopan@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Yue Zhang (XH_31829)
  { updateOne: {
      filter: { email: { $regex: /^yuez900228@gmail.com$/i } },
      update: { $set: { keyword1: "Elderly care", keyword2: "Residential care" } }
  }},
  // Yvonne Keppel Kerr (XH_32197)
  { updateOne: {
      filter: { email: { $regex: /^keppely@gmail.com$/i } },
      update: { $set: { keyword1: "Acute hospital wards", keyword2: "Post-operative care" } }
  }},
  // Yvonne Majongosi ()
  { updateOne: {
      filter: { email: { $regex: /^yvonnemajongos@gmail.com$/i } },
      update: { $set: { keyword1: "Manual handling", keyword2: "Chronic illness management" } }
  }},
  // Zanele Dube ()
  { updateOne: {
      filter: { email: { $regex: /^zaneledube777@gmail.com$/i } },
      update: { $set: { keyword1: "Chronic illness management", keyword2: "Post-operative care" } }
  }},
];

const result = db.staffs.bulkWrite(ops, { ordered: false });
print("Matched  : " + result.matchedCount);
print("Modified : " + result.modifiedCount);
print("Unmatched: " + (ops.length - result.matchedCount));