# TODO: Implement function logic
# function arguments should be R dataframes, with their names equal to what is defined in the python data container file, by the specific methods

funcInicializa <- function(matriz2_og, matrizB_og, matrizA_og, startDate, endDate) {
  
  # startDate <- startDate2
  # endDate <- endDate2
  
  ano <- year(min(matrizB_og$DATA))
  matrizB_ini <- matrizB_og %>% 
    dplyr::mutate(minTurno = case_when(
      DATA %in% c(paste0(ano,'-12-23'),
                  paste0(ano,'-12-24'),
                  paste0(ano,'-12-30'),
                  paste0(ano,'-12-31')) ~ maxTurno,
      DATA %in% c(paste0(ano,'-12-22'),paste0(ano,'-12-29')) & TURNO == 'M' ~ maxTurno,
      T ~ minTurno
    ))
  
  
  #CRIAR MATRIZ_2--------------------------------------------------
  
  # matriz2_og <- as.data.frame(matriz2_og)
  names(matriz2_og) <- paste0(matriz2_og[which(matriz2_og[,1]=='Dia'),],"_",matriz2_og[which(matriz2_og[,1]=='TURNO'),])
  names(matriz2_og)[1] <- 'DIA_TURNO'
  matriz2_ini <- reshape2::melt(matriz2_og,id.vars='DIA_TURNO')
  names(matriz2_ini) <- c("COLABORADOR","DATA","TIPO_TURNO")
  
  matriz2_ini <- matriz2_ini %>% 
    dplyr::mutate(DATA = as.character(DATA)) %>% 
    dplyr::filter(DATA >= startDate, DATA <= endDate)
  
  ##filtrar linhas que nao interessam para matriz2
  matriz2 <- matriz2_ini %>% 
    dplyr::filter(!(COLABORADOR %in% c("Dia","maxTurno","mediaTurno","minTurno","sdTurno"))) %>% 
    dplyr::filter(COLABORADOR != 'TURNO')
  
  matriz2$DATA <- sub("_.*$", "", matriz2$DATA)
  
  
  ##adicionar coluna com resultado da atribuicao de L
  matriz2 <- matriz2 %>% 
    dplyr::mutate(HORARIO = ifelse(TIPO_TURNO %in% c('M','T', 'MoT', 'P'),'H', TIPO_TURNO))
  
  tiposDeTurno <-  unique(matriz2$TIPO_TURNO)
  
  
  
  if ('MoT' %in% unique(matriz2$TIPO_TURNO)){ matriz2 <- funcTurnos(matriz2, 'MoT') }
  
  if ('P' %in% unique(matriz2$TIPO_TURNO)){ matriz2 <- funcTurnos(matriz2, 'P') }
  
  
  matriz2$WDAY <- wday(as.Date(matriz2$DATA))
  
  matriz2 <- matriz2 %>% 
    dplyr::mutate(ID = row_number(),
                  WW = isoweek(as.Date(DATA, format = '%Y-%m-%d')))
  
  matriz2$WD <- lubridate::wday(as.character(as.POSIXct(matriz2$DATA, format = '%Y-%m-%d')), label=T, abbr = T)
  matriz2$WD <- as.character(matriz2$WD)
  matriz2 <- matriz2 %>% 
    dplyr::group_by(DATA) %>% 
    dplyr::mutate(DIA_TIPO = ifelse(((any(TIPO_TURNO == "F") | WD == "Sun") & HORARIO != 'F'), 'domYf',WD)) %>% 
    ungroup()
  
  # if (any(grepl('L',matriz2$HORARIO , ignore.case = TRUE))) {
    # matriz24444 <- matriz2 %>%
    #   dplyr::mutate(WD = wday(DATA)) %>%
    #   dplyr::arrange(COLABORADOR, DATA) %>%
    #   dplyr::group_by(COLABORADOR) %>%
    #   dplyr::summarise(DyF_MAX_T_at = sum(DIA_TIPO=='domYf' & HORARIO=='L_DOM')/2,
    #                    Q_at = sum(HORARIO=='LQ'),
    #                    C2D_at = sum((HORARIO=='L' & lead(HORARIO)=='L_DOM' & lead(WD)==1) |
    #                                   (HORARIO=='L' & lag(HORARIO)=='L_DOM') & lag(WD)==1)/2,
    #                    C3D_at = sum( (lag(HORARIO)=='L' & HORARIO=='L_DOM' & lead(HORARIO)=='L') |
    #                                    (lag(HORARIO,2)=='L' & lag(HORARIO)=='L_DOM' & HORARIO=='L'))/2,
    #                    CXX_at = sum((HORARIO=='L' & WD %in% c(3,4,5)) |
    #                                   (HORARIO=='L' & lag(HORARIO)!='L'& WD %in% c(2)) |
    #                                   (HORARIO=='L' & lead(HORARIO,2)!='L'& WD %in% c(6)) |
    #                                   (HORARIO=='L' & lag(HORARIO)!='L'& WD %in% c(7))
    #                    )/2,
    #                    LQ_at = sum(HORARIO=='LQ'),
    #                    LD_at = sum(HORARIO=='L')/2,
    #                    .groups='drop') %>% data.frame()

  #   
  #   tstst <- matriz2 %>% 
  #     dplyr::filter(COLABORADOR == 4094507)
  #   
  #   tstst %>% 
  #     # dplyr::filter(grepl('H|NL',HORARIO , ignore.case = TRUE)) %>%
  #     dplyr::mutate(WD = wday(DATA)) %>% 
  #     dplyr::arrange(COLABORADOR, DATA) %>% 
  #     dplyr::group_by(COLABORADOR, DATA) %>% 
  #     dplyr::mutate(dups = row_number()) %>% 
  #     dplyr::filter(dups==1) %>%# View()
  #     dplyr::group_by(COLABORADOR) %>% 
  #     dplyr::summarise(DyF_MAX_T_at = sum(DIA_TIPO=='domYf' & HORARIO=='L_DOM'),
  #                      LD_at = sum(DIA_TIPO=='domYf' & HORARIO=='H'))
  # }
  
  
  #CRIAR MATRIZ_A--------------------------------------------------
  
  ## Contador Liberancas de Domingo -------------------------------
  ### Lista de Feriados Abertos ---------------------------------
  
  ##extrair festivos
  domEfes <- matriz2 %>% 
    dplyr::filter((HORARIO=='F'), COLABORADOR == 'TIPO_DIA') %>% 
    unique()
  ##extrair domngos e festivos com pessoas a trabalhar
  trabM2 <- matriz2 %>% 
    dplyr::filter(DATA %in% domEfes$DATA ) %>% 
    dplyr::group_by(DATA) %>% 
    #dplyr::filter(any(HORARIO == 'H'))
    dplyr::filter((HORARIO == 'H'))
  
  
  matrizA_og <- matrizA_og %>% filter(MATRICULA != "") 
  df_merge_count_dom_fes <- data.frame(MATRICULA = character(), TOTAL_DOM_FES = numeric())
  df_merge_count_fes <- data.frame(MATRICULA = character(), TOTAL_FES = numeric())
  df_merge_count_holidays <- data.frame(MATRICULA = character(), TOTAL_HOLIDAYS = numeric())
  ################################
  # Confirma que na matriz2 cada #
  # Vez que há um V marcado está #
  # marcado no turno da manhã e  #                                                                               
  # da tarde em simultâneo       #                                                                               
  ################################                                                                               
  
  matriz2_V_only <- matriz2 %>% dplyr::filter(HORARIO %in% c('L','V'))                                                  
  
  garante_V_manha_tarde <- all(duplicated(matriz2_V_only) | duplicated(matriz2_V_only, fromLast = TRUE))       
  
  
  #________________________________________________________________________________
  # Confirma que na matriz2 cada 
  # Vez que há um V ou Lmarcado está 
  # marcado no turno da manhã e  
  # da tarde em simultâneo    
  
  ##AVALIAR SE 'V' DEVE SER CONTABILIZADO PARA DESCONTAR NOS L_TOTAL OU NAO!!!
  ## L e V so deve ser descontado na Almcapo
  ## na sabeco so se desconta os L
  
  if (unique(matrizA_og$CONVENIO) =='ALCAMPO') {
    for (matricula in matrizA_og$MATRICULA) {
      count_occurrences <- 0
      count_sundaysInHolidays <- 0
      tipo_contrato <-  matrizA_og %>% 
        dplyr::filter(MATRICULA == matricula) %>% .$TIPO_CONTRATO
      matriz_temp <- matriz2 %>% 
        dplyr::filter(COLABORADOR == matricula)
      
      if (tipo_contrato %in% c(4,5)) {
        count_occurrences <- matriz_temp %>% 
          dplyr::filter(HORARIO %in% c('L','V') & ( WDAY == 1 )) %>%
          # dplyr::filter(HORARIO %in% c('L') & ( WDAY == 1 | DATA %in% domEfes$DATA)) %>% 
          nrow()
        count_occurrences_fes <- matriz_temp %>% 
          dplyr::filter(HORARIO %in% c('L','V') & ( DATA %in% domEfes$DATA )) %>%
          # dplyr::filter(HORARIO %in% c('L') & ( WDAY == 1 | DATA %in% domEfes$DATA)) %>% 
          nrow()
        count_Holidays <- matriz_temp %>% 
          dplyr::filter(HORARIO %in% c('V')) %>%
          # dplyr::filter(HORARIO %in% c('L') & ( WDAY == 1 | DATA %in% domEfes$DATA)) %>% 
          nrow()
      } else{
        count_occurrences <- matriz_temp %>% 
          dplyr::filter(HORARIO %in% c('L','V') & ( WDAY == 1 | DATA %in% domEfes$DATA)) %>%
          # dplyr::filter(HORARIO %in% c('L') & ( WDAY == 1 | DATA %in% domEfes$DATA)) %>% 
          nrow()
        count_occurrences_fes <- 0
        count_Holidays <- 0
      }


      temp_df <- data.frame(MATRICULA = matricula, TOTAL_DOM_FES = count_occurrences )
      temp_df_fes <- data.frame(MATRICULA = matricula, TOTAL_FES = count_occurrences_fes )
      temp_df_Holidays <-  data.frame(MATRICULA = matricula, TOTAL_HOLIDAYS = count_Holidays )
      df_merge_count_dom_fes <- rbind(df_merge_count_dom_fes, temp_df)
      df_merge_count_fes <- rbind(df_merge_count_fes, temp_df_fes)
      df_merge_count_holidays <- rbind(df_merge_count_holidays, temp_df_Holidays)
      
      
      
      print(paste(matricula, count_occurrences,count_occurrences_fes,count_Holidays))
      
    }
  }else{
    for (matricula in matrizA_og$MATRICULA) {
      count_occurrences <- 0
      matriz_temp <- matriz2 %>% 
        dplyr::filter(COLABORADOR == matricula)
      count_occurrences <- matriz_temp %>% 
        dplyr::filter(grepl('L_',HORARIO , ignore.case = TRUE) & ( WDAY == 1 | DATA %in% domEfes$DATA)) %>%
        # dplyr::filter(HORARIO %in% c('L') & ( WDAY == 1 | DATA %in% domEfes$DATA)) %>% 
        nrow()
      temp_df <- data.frame(MATRICULA = matricula, TOTAL_DOM_FES = count_occurrences )
      df_merge_count_dom_fes <- rbind(df_merge_count_dom_fes, temp_df)
      
      
      
      print(paste(matricula, count_occurrences))
      
    }
  }
  

  
  df_merge_count_dom_fes <- df_merge_count_dom_fes %>% filter(MATRICULA != "") 
  df_merge_count_dom_fes <- distinct(df_merge_count_dom_fes)
  df_merge_count_fes <- df_merge_count_fes %>% filter(MATRICULA != "") 
  df_merge_count_fes <- distinct(df_merge_count_fes)
  df_merge_count_holidays <- df_merge_count_holidays %>% filter(MATRICULA!="")
  df_merge_count_holidays <- distinct(df_merge_count_holidays)
  
  
  matrizA_og <- distinct(matrizA_og)
  matrizA_og <- merge(matrizA_og, df_merge_count_dom_fes, by = 'MATRICULA')
  matrizA_og <- merge(matrizA_og, df_merge_count_fes, by = 'MATRICULA')
  matrizA_og <- merge(matrizA_og, df_merge_count_holidays, by = 'MATRICULA')
  matrizA_og$TOTAL_DOM_FES <- matrizA_og$TOTAL_DOM_FES/2
  matrizA_og$TOTAL_FES <- matrizA_og$TOTAL_FES/2
  matrizA_og$TOTAL_HOLIDAYS <- matrizA_og$TOTAL_HOLIDAYS/2
  
  ## FIM Liberancas de Domingo ---------------------------------------------------
  
  
  matrizA_og <- matrizA_og %>% dplyr::mutate(TOTAL_DOM_FES = ifelse(TIPO_CONTRATO == 3, 0, TOTAL_DOM_FES))
  matrizA_og <- matrizA_og %>% dplyr::mutate(DyF_MAX_T = ifelse(TIPO_CONTRATO == 3, 0, DyF_MAX_T))
  
  ## criar coluna de descansos atribuidos para controlo/desempate 
  matrizA_og$DESCANSOS_ATRB <- 0
  
  # CALCULA LIBRANÇAS ------------------------------------------------------------
  # ### CONTRATOS DE 4/5 DIAS DESCONTA DE L_RES
  # colabs_45D <- matrizA_og %>% dplyr::filter(TIPO_CONTRATO %in% c(4,5))
  # countLDTst_45D <- matriz2 %>%
  #   dplyr::filter(COLABORADOR %in% colabs_45D$MATRICULA) %>% 
  #   dplyr::mutate(WD = wday(DATA)) %>%
  #   dplyr::arrange(COLABORADOR, DATA, desc(HORARIO)) %>%
  #   dplyr::group_by(COLABORADOR, DATA) %>%
  #   dplyr::mutate(dups = row_number()) %>% ungroup() %>% 
  #   dplyr::filter(dups==1) %>% #View()#dplyr::mutate(ff = lag(HORARIO), ff2 = lead(HORARIO),
  #                               #             x= DIA_TIPO!='domYf' & HORARIO=='L' & WD %in% c(2,3,4,5,6,7) & (lag(HORARIO)!='L' & lead(HORARIO)!='L' )) %>% View()
  #   dplyr::group_by(COLABORADOR) %>% 
  #   dplyr::summarise(
  #     LD_at = 0,
  #     LRES_at = sum((DIA_TIPO!='domYf' & HORARIO=='L' & WD %in% c(2,3,4,5,6,7) & (lag(HORARIO,default = 'H')!='L' & lead(HORARIO,default = 'H')!='L' ))),
  #       # + sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD ==2 & lag(HORARIO)!='L') +
  #       # sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD ==7 & lead(HORARIO)!='L'),
  #     cxx = sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD %in% c(2,3,4,5,6) & (lead(HORARIO)=='L' )),
  #     .groups='drop') %>% data.frame() %>% 
  #   dplyr::mutate(across(everything(), ~ ifelse(is.na(.), 0, .)))
  
  # ### CONTRATOS DE 6 DIAS DESCONTA DE L_D (ou L_Q ? >> duvida)
  # colabs_6D <- matrizA_og %>% dplyr::filter(TIPO_CONTRATO %in% c(6))
  # countLDTst_6D <- matriz2 %>%
  #   dplyr::filter(COLABORADOR %in% colabs_6D$MATRICULA) %>% 
  #   dplyr::mutate(WD = wday(DATA)) %>%
  #   dplyr::arrange(COLABORADOR, DATA, desc(HORARIO)) %>%
  #   dplyr::group_by(COLABORADOR, DATA) %>%
  #   dplyr::mutate(dups = row_number()) %>% ungroup() %>% 
  #   dplyr::filter(dups==1) %>% 
  #   dplyr::group_by(COLABORADOR) %>% 
  #   dplyr::summarise(
  #     LD_at = sum((DIA_TIPO!='domYf' & HORARIO=='L' & WD %in% c(2,3,4,5,6,7) & (lag(HORARIO,default = 'H')!='L' & lead(HORARIO,default = 'H')!='L' ))), 
  #       # + sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD ==2 & lag(HORARIO)!='L') +
  #       # sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD ==7 & lead(HORARIO)!='L'),
  #     LRES_at = 0,
  #     .groups='drop') %>% data.frame() %>% 
  # dplyr::mutate(across(everything(), ~ ifelse(is.na(.), 0, .)))
  # 
  # countLDTst <- countLDTst_6D %>%
  #   dplyr::mutate(
  #     COLABORADOR = as.character(COLABORADOR),  # Certificar que é do mesmo tipo
  #     LD_at = as.numeric(LD_at),                # Converter para numérico
  #     LRES_at = as.numeric(LRES_at)             # Converter para numérico
  #   ) %>% 
  #   dplyr::bind_rows(countLDTst_45D %>%
  #                      dplyr::mutate(
  #                        COLABORADOR = as.character(COLABORADOR),  # Certificar que é do mesmo tipo
  #                        LD_at = as.numeric(LD_at),                # Converter para numérico
  #                        LRES_at = as.numeric(LRES_at)             # Converter para numérico
  #                      ))
  
  df_CD <-matriz2 %>%
    dplyr::filter(COLABORADOR %in% matrizA_og$MATRICULA) %>% 
    dplyr::mutate(WD = wday(DATA)) %>%
    dplyr::arrange(COLABORADOR, DATA) %>%
    dplyr::group_by(COLABORADOR, DATA) %>%
    dplyr::mutate(dups = row_number()) %>% ungroup() %>% 
    dplyr::filter(dups==1) %>%
    dplyr::group_by(COLABORADOR) %>%
    mutate(
      TIPO_TURNO_NEXT = lead(HORARIO,default = 'H'),       # Next day's shift
      TIPO_TURNO_PREV = lag(HORARIO,default = 'H'),       # Prev day's shift
      WDAY_NEXT = lead(WDAY),                   # Next day's weekday
      TIPO_TURNO_NEXT2 = lead(HORARIO, n=2,default = 'H'),   # Shift for the day after next (2 days later)
      WDAY_NEXT2 = lead(WDAY, n=2),
      WDAY_PREV = lag(WDAY)# Weekday for the day after next
    )
  
  
  ### CONTRATOS DE 4/5 DIAS DESCONTA DE L_RES
  colabs_45D <- matrizA_og %>% dplyr::filter(TIPO_CONTRATO %in% c(4,5))
  countLDTst_45D <- df_CD %>% 
    dplyr::filter(COLABORADOR %in% colabs_45D$MATRICULA) %>%
    group_by(COLABORADOR) %>%
    dplyr::summarise(
      LD_at = 0,
      LRES_at = sum((DIA_TIPO!='domYf' & HORARIO=='L' & WD %in% c(2,3,4,5,6,7) & (lag(HORARIO,default = 'H')!='L' & lead(HORARIO,default = 'H')!='L' ))),
      # + sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD ==2 & lag(HORARIO)!='L') +
      # sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD ==7 & lead(HORARIO)!='L'),
      CXX_at = sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD %in% c(2,3,4,5,6) & (lead(HORARIO)=='L' )),
      .groups='drop') %>% data.frame() %>% 
    dplyr::mutate(across(everything(), ~ ifelse(is.na(.), 0, .)))
  
  ### CONTRATOS DE 6 DIAS DESCONTA DE L_D (ou L_Q ? >> duvida)
  colabs_6D <- matrizA_og %>% dplyr::filter(TIPO_CONTRATO %in% c(6))
  countLDTst_6D <- df_CD %>%
    dplyr::filter(COLABORADOR %in% colabs_6D$MATRICULA) %>% 
    dplyr::group_by(COLABORADOR) %>% 
    dplyr::summarise(
      LD_at = sum((DIA_TIPO!='domYf' & HORARIO=='L' & WD %in% c(2,3,4,5,6,7) & (lag(HORARIO,default = 'H')!='L' & lead(HORARIO,default = 'H')!='L' ))), 
      # + sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD ==2 & lag(HORARIO)!='L') +
      # sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD ==7 & lead(HORARIO)!='L'),
      LRES_at = 0,
      CXX_at = sum(DIA_TIPO!='domYf' & HORARIO=='L' & WD %in% c(2,3,4,5,6) & (lead(HORARIO)=='L' )),
      .groups='drop') %>% data.frame() %>% 
    dplyr::mutate(across(everything(), ~ ifelse(is.na(.), 0, .)))
  
  ### JUNTAR TODOS COLABS
  countLDTst <- countLDTst_6D %>%
    dplyr::mutate(
      COLABORADOR = as.character(COLABORADOR),  # Certificar que é do mesmo tipo
      LD_at = as.numeric(LD_at),                # Converter para numérico
      LRES_at = as.numeric(LRES_at)             # Converter para numérico
    ) %>% 
    dplyr::bind_rows(countLDTst_45D %>%
                       dplyr::mutate(
                         COLABORADOR = as.character(COLABORADOR),  # Certificar que é do mesmo tipo
                         LD_at = as.numeric(LD_at),                # Converter para numérico
                         LRES_at = as.numeric(LRES_at)             # Converter para numérico
                       ))
  
  # Identify where:
  # 1. Saturday-Sunday-Monday occurs (L -> L_DOM -> L)
  # 2. Saturday-Sunday (L -> L_DOM) occurs without Monday, or Sunday-Monday (L_DOM -> L) alone
  
  df_CD <- df_CD %>%
    mutate(
      # Friday -> Saturday -> Sunday (3-day sequence)
      FRI_SAT_SUN = (WDAY_PREV == 6 & HORARIO == 'L' & WDAY == 7 & TIPO_TURNO_NEXT == 'L' & WDAY_NEXT == 1 & TIPO_TURNO_NEXT2 == 'L_DOM'),
      
      
      # Saturday -> Sunday -> Monday (3-day sequence)
      SAT_SUN_MON = (WDAY == 7 & HORARIO == 'L' & WDAY_NEXT == 1 & TIPO_TURNO_NEXT == 'L_DOM' & WDAY_NEXT2 == 2 & TIPO_TURNO_NEXT2 == 'L'),
      
      # Saturday -> Sunday (2-day sequence) not followed by Monday
      SAT_SUN_ONLY = #(WDAY == 7 & TIPO_TURNO == 'L' & WDAY_NEXT == 1 & TIPO_TURNO_NEXT == 'L_DOM' & (is.na(WDAY_NEXT2) | WDAY_NEXT2 != 2)),
        (WDAY == 7 & HORARIO == 'L' & WDAY_NEXT == 1 & TIPO_TURNO_NEXT == 'L_DOM' & WDAY_NEXT2 == 2 & TIPO_TURNO_NEXT2 != 'L'),
      
      # Sunday -> Monday (2-day sequence) not preceded by Saturday
      SUN_MON_ONLY = #(WDAY == 1 & TIPO_TURNO == 'L_DOM' & WDAY_NEXT == 2 & TIPO_TURNO_NEXT == 'L' & (is.na(WDAY_PREV) | WDAY_PREV != 7) )
        (WDAY == 7 & HORARIO != 'L' & WDAY_NEXT == 1 & TIPO_TURNO_NEXT == 'L_DOM' & WDAY_NEXT2 == 2 & TIPO_TURNO_NEXT2 == 'L'),
    )
  
  # Filter and count the occurrences of the patterns
  ###C3D
  
  C3D_FRI_SAT_SUN <- df_CD %>%
    group_by(COLABORADOR) %>%  # Group by COLABORADOR
    filter(FRI_SAT_SUN) %>%    # Apply the condition
    summarise(C3D_at_FSS = n_distinct(DATA), .groups = 'drop')  # Count distinct DATA for each COLABORADOR
  
  C3D_SAT_SUN_MON <- df_CD %>%
    group_by(COLABORADOR) %>%  # Group by COLABORADOR
    filter(SAT_SUN_MON) %>%    # Apply the condition
    summarise(C3D_at_SSM = n_distinct(DATA), .groups = 'drop')
  
  C3D_at <- full_join(C3D_SAT_SUN_MON,C3D_FRI_SAT_SUN,by = 'COLABORADOR' ) 
  C3D_at <- C3D_at %>% 
    dplyr::mutate(C3D_at = coalesce(C3D_at_SSM,0) + coalesce(C3D_at_FSS,0)) %>% 
    dplyr::select(-C3D_at_SSM,-C3D_at_FSS)
  
  
  ###C2D
  
  C2D_SAT_SUN_ONLY <- df_CD %>%
    filter(SAT_SUN_ONLY) %>%
    summarise(C2D_at_SSO = n_distinct(DATA))
  
  C2D_SUN_MON_ONLY <- df_CD %>%
    filter(SUN_MON_ONLY) %>%
    summarise(C2D_at_SMO = n_distinct(DATA))
  
  C2D_at <- full_join(C2D_SAT_SUN_ONLY,C2D_SUN_MON_ONLY,by = 'COLABORADOR' ) 
  C2D_at <- C2D_at %>% 
    dplyr::mutate(C2D_at = coalesce(C2D_at_SSO,0) + coalesce(C2D_at_SMO,0)) %>% 
    dplyr::select(-C2D_at_SSO,-C2D_at_SMO) %>% 
    dplyr::full_join(C3D_at, by = 'COLABORADOR') %>% 
    dplyr::mutate(across(everything(), ~ replace_na(.,0))) %>% 
    data.frame()
  
  
  
  countLDTst <-   countLDTst %>% 
    dplyr::full_join(C2D_at, by = 'COLABORADOR') %>% 
    dplyr::mutate(across(everything(), ~ replace_na(.,0)))
  
  
  matrizA_og <- matrizA_og %>% 
    merge(countLDTst, by.x = "MATRICULA", by.y = "COLABORADOR", all.x = T) %>% 
    dplyr::mutate_all(~ ifelse(is.na(.), 0, .))
  
  
  matrizA_og<- matrizA_og %>% 
    dplyr::group_by(FK_COLABORADOR) %>% 
    dplyr::mutate(
      L_TOTAL = L_TOTAL - LD_at,
      ## tratar LD ---------------
      LD = max(LD - LD_at,0),
      ## tratar L_DOM ---------------
      L_DOM = L_DOM - TOTAL_DOM_FES - TOTAL_FES, #- 4
     
      # matrizA_og$LD <- matrizA_og$DyF_MAX_T
      L_TOTAL = L_TOTAL - TOTAL_DOM_FES,
      ## tratar Ferias ----------
      L_TOTAL = L_TOTAL - custom_round(TOTAL_HOLIDAYS/7),
      
      ## tratar C2D e C3D ---------------
      L_TOTAL = L_TOTAL - C2D_at,
      C2D = max(C2D - C2D_at,0),
      
      L_TOTAL = L_TOTAL - C3D_at,
      C3D = max(C3D - C3D_at,0),
      
      ## L_RES TRATADO MAIS A FRENTE ---------------,
    ) %>% ungroup()
  
  
  matrizA <- matrizA_og %>%  select(UNIDADE,SECAO,POSTO, FK_COLABORADOR,MATRICULA, OUT,
                                    TIPO_CONTRATO, CICLO, L_TOTAL, L_DOM, LD, LQ,Q,C2D,C3D,CXX, DESCANSOS_ATRB, 
                                    DyF_MAX_T, LRES_at)
  
  
  ###-----------------------------------------------
  
  
  #### alteracoes 07/12
  ## CONTRATOS 2/3DIAS
  
  ## comentado dia 26/09/24 -> adicionada logica no fim desta func
  matriz2_3D <- matriz2 %>%
    #dplyr::filter(COLABORADOR %in% ((matrizA %>%  dplyr::filter(TIPO_CONTRATO %in% c(2,3)))  %>% .$MATRICULA)) %>%
    merge(matrizA %>% dplyr::filter(TIPO_CONTRATO %in% c(2,3)) %>% dplyr::select(MATRICULA,TIPO_CONTRATO), by.x = 'COLABORADOR', by.y='MATRICULA' ) %>%
    
    dplyr::group_by(WW,COLABORADOR) %>%
    dplyr::filter(!(HORARIO %in% c("-", "V", "F"))) %>%
    group_by(COLABORADOR, WW) %>%
    mutate(count = n()/2)  %>%
    ungroup()


  matriz2 <- merge(matriz2, matriz2_3D, by=c("COLABORADOR","DATA","TIPO_TURNO" , "HORARIO", "ID", "WDAY", "WW" ,"WD","DIA_TIPO"),all = T)

  matriz2 <- matriz2 %>%
    dplyr::mutate(HORARIO = case_when(
      (count == 3 ) & TIPO_CONTRATO==3~ "NL3D",
      (count == 2 ) & TIPO_CONTRATO==2~ "NL2D",
      #count == 4 & DIA_TIPO == 'domYf' ~ "NL3D",
      T ~ HORARIO
    )) %>% dplyr::select(-count)

  matriz2$DATA <- as.character(matriz2$DATA)

  matriz2 <- as_tibble(matriz2)


  matriz3D <- matriz2 %>%
    dplyr::filter(COLABORADOR %in% ((matrizA %>%  dplyr::filter(TIPO_CONTRATO %in% c(2,3)))  %>% .$MATRICULA))%>%

    dplyr::filter(!(HORARIO %in% c("-", "V", "F", "NL3D","NL2D"))) %>%
    dplyr::group_by(COLABORADOR) %>%
    mutate(count = n_distinct(WW)) %>%
    group_by(COLABORADOR) %>%
    slice(1) %>%
    dplyr::select(COLABORADOR, count) %>%
    dplyr::rename(MATRICULA = COLABORADOR)


  matrizA <- merge(matrizA, matriz3D, by="MATRICULA", all=T)


  matrizA<- matrizA %>% dplyr::mutate(L_TOTAL = ifelse(TIPO_CONTRATO %in% c(2,3), count, L_TOTAL))
  # 
  
  
  
  ###Está um colab a ir de grelo aqui!!!!!!!!!!!!!!!!!!!!!
  ########################################################
  ########################################################
  ########################################################
  ########################################################
  ########################################################
  ########################################################
  ########################################################
  ########################################################
  ########################################################
  ########################################################
  matrizA <- matrizA %>% 
    # dplyr::filter(!is.na(L_TOTAL), L_TOTAL > 0) %>%
    dplyr::mutate_all(~ ifelse(is.na(.), 0, .)) %>% 
    dplyr::select(-count) %>%
    mutate_all(~replace(., . < 0, 0))
  
  matrizA$L_RES <- 0
  
  matrizA <- matrizA %>% dplyr::mutate(aux =L_DOM +LD+ LQ+C2D+C3D+CXX)
  
  matrizA <- matrizA %>% 
    dplyr::mutate(aux2 =  L_TOTAL - aux) %>% 
    dplyr::mutate(LD = ifelse(aux2 < 0, LD + aux2, LD)) %>% 
    dplyr::select(-c(aux, aux2))
  
  
  matrizA <- matrizA %>% dplyr::mutate(L_RES = ifelse(TIPO_CONTRATO == 3, L_TOTAL, L_RES))
  
  matrizA <- matrizA %>%
    dplyr::rename(L_D = LD,
                  L_Q = LQ,
                  L_QS = Q)
  
  ### adiciona dias vazios aos 4D
  
  matrizA <- matrizA %>% 
    dplyr::mutate(VZ = case_when(
      TIPO_CONTRATO == 4 ~ 52-4, #4SEMANAS COM FERIAS
      T ~ 0
    ))
  
  
  
  matriz2 <- matriz2 %>% 
    dplyr::mutate(HORARIO = ifelse(HORARIO == 'L', 'L_', HORARIO))
  
  
  matrizA_bk_OG <- matrizA %>% dplyr::mutate(L_RES = L_TOTAL-L_DOM-L_D-L_Q-L_QS-C2D-C3D-CXX-VZ-LRES_at,
                                             L_TOTAL = L_TOTAL - LRES_at) %>% dplyr::select(-LRES_at)
 
  matrizA_bk <- matrizA %>% dplyr::mutate(L_RES = L_TOTAL-L_DOM-L_D-L_Q-L_QS-C2D-C3D-CXX-VZ-LRES_at,
                                          L_TOTAL = L_TOTAL - LRES_at) %>% dplyr::select(-LRES_at)
  
  matriz_data_turno_bk <- data.table(COLUNA=NA)
  
  matrizA_bk <- matrizA_bk %>% 
    dplyr::arrange(desc(L_TOTAL)) %>%
    mutate_all(~replace(., . < 0, 0))
  

  # Dev TIPO TURNO FIX ---------------------------------------------------------------------
    #temporary Dev to keep only first row of TIPO_TURNO
    # new_row_m2 <- matriz2[42,]
    # new_row_m2$TIPO_TURNO <- "T"
    # matriz2_teste <- rbind(matriz2, new_row_m2)
  # matriz2_toDelete <<- matriz2
  matriz2_TipoTurnoFix <- matriz2 %>%
    dplyr::filter(HORARIO == 'H') %>% 
    dplyr::select(WW,TIPO_TURNO) %>% 
    unique()
  matriz2_TipoTurnoFix <- matriz2_TipoTurnoFix %>% 
    dplyr::distinct(WW, .keep_all = T) %>% 
    dplyr::rename(TIPO_TURNO_FIX = TIPO_TURNO)
  
  matriz2 <- matriz2 %>% 
    dplyr::left_join(matriz2_TipoTurnoFix, by = 'WW') %>% 
    dplyr::mutate(TIPO_TURNO = case_when(TIPO_TURNO == 'NL' ~ TIPO_TURNO_FIX,
                                          TRUE ~ TIPO_TURNO))
  matriz2_bk <- matriz2 
  
  # View(semanasTrabalho %>% reshape2::dcast( COLABORADOR+TIPO_CONTRATO ~ WW, value.var = "diasH") )
  
  for (colab in matrizA_bk$MATRICULA) {
    # colab <- matrizA_bk$MATRICULA[1]
    print(colab)
    mmA <- matrizA_bk %>% dplyr::filter(MATRICULA==colab)
    # print(mmA)
    if (mmA$TIPO_CONTRATO==2) {
      
      newC <- matriz2_bk %>%
        dplyr::filter(COLABORADOR == colab) %>%
        dplyr::arrange(COLABORADOR, DATA, desc(TIPO_TURNO)) %>%
        dplyr::group_by(COLABORADOR, DATA) %>%
        dplyr::mutate(dups = row_number()) %>% ungroup() %>% #View()
        dplyr::filter(dups==1) %>% #View()
        dplyr::group_by(COLABORADOR, WW) %>%
        do(calcular_folgas2(.)) %>% ungroup() %>% 
        dplyr::group_by(COLABORADOR) %>% 
        dplyr::summarise(L_RES = sum(L_RES),
                         L_DOM = sum(L_DOM),
                         L_TOTAL = L_RES+L_DOM, .groups='drop')
      
      mmA$L_RES <- newC$L_RES
      mmA$L_DOM <- newC$L_DOM
      mmA$L_TOTAL <- newC$L_TOTAL
      
      matrizA_bk <- matrizA_bk %>% 
        dplyr::filter(MATRICULA != colab) %>% 
        dplyr::bind_rows(mmA)
      
    } else if (mmA$TIPO_CONTRATO==3) {
      newC <- matriz2_bk %>%
        dplyr::filter(COLABORADOR == colab) %>%
        dplyr::arrange(COLABORADOR, DATA, desc(TIPO_TURNO)) %>%
        dplyr::group_by(COLABORADOR, DATA) %>%
        dplyr::mutate(dups = row_number()) %>% ungroup() %>% #View()
        dplyr::filter(dups==1) %>% #View()
        dplyr::group_by(COLABORADOR, WW) %>%
        do(calcular_folgas3(.)) %>% ungroup() %>% 
        dplyr::group_by(COLABORADOR) %>% 
        dplyr::summarise(L_RES = sum(L_RES),
                         L_DOM = sum(L_DOM),
                         L_TOTAL = L_RES+L_DOM, .groups='drop')
      
      mmA$L_RES <- newC$L_RES
      mmA$L_DOM <- newC$L_DOM
      mmA$L_TOTAL <- newC$L_TOTAL
      
      matrizA_bk <- matrizA_bk %>% 
        dplyr::filter(MATRICULA != colab) %>% 
        dplyr::bind_rows(mmA)
    } else if (mmA$DyF_MAX_T == 0 & mmA$CICLO != 'COMPLETO') {
      
      
      #devido ao espaçamento dos domingos, quando nao trabalha nenhu
      #e preciso forcar todos os domYf a L e retirar os respectivos L_DOM a atribuir
      mmA$L_TOTAL <- mmA$L_TOTAL - mmA$L_DOM
      mmA$L_DOM <- 0
      
      matrizA_bk <- matrizA_bk %>% 
        dplyr::filter(MATRICULA != colab) %>% 
        dplyr::bind_rows(mmA)
      
      
      #atualizar matriz calendario com domYfes com L
      newC <- matriz2_bk %>%
        dplyr::filter(COLABORADOR == colab) %>% 
        dplyr::mutate(HORARIO =case_when(
          DIA_TIPO=='domYf' & HORARIO != 'V' ~ 'L_DOM',
          T ~ HORARIO
        ) ) 
      
      matriz2_bk <- matriz2_bk %>% 
        dplyr::filter(COLABORADOR != colab) %>% 
        dplyr::bind_rows(newC)
      
      
    ######ADDED DEV TO RESET L values in case CICLO is of type complete-----  
    }
    if (mmA$CICLO == 'COMPLETO') {
      
      mmA[, 9:ncol(mmA)] <- 0
      
      matrizA_bk <- matrizA_bk %>% 
        dplyr::filter(MATRICULA != colab) %>% 
        dplyr::bind_rows(mmA)
    }
    
    if (mmA$TIPO_CONTRATO %in% c(4,5) & mmA$CXX > 0) {
      
      mmA$L_RES2 <- mmA$L_RES - mmA$CXX
      mmA$L_RES <- mmA$CXX
      
      matrizA_bk <- matrizA_bk %>% 
        dplyr::filter(MATRICULA != colab) %>% 
        dplyr::bind_rows(mmA)
    } else{
      mmA$L_RES2 <- 0
      
      matrizA_bk <- matrizA_bk %>% 
        dplyr::filter(MATRICULA != colab) %>% 
        dplyr::bind_rows(mmA)
    }
    
  }
  
  
  matrizA_bk <- matrizA_bk %>% ungroup() %>% dplyr::select(-DyF_MAX_T) %>% data.frame()
  
  
  logs <- NULL
  
  matriz2_bk <- matriz2_bk %>% 
    dplyr::mutate(HORARIO =case_when(
    DIA_TIPO=='domYf' & HORARIO == 'L_' ~ 'L_DOM',
    T ~ HORARIO
  ) ) 
  
  #CRIAR MATRIZ_B--------------------------------------------------
  
  ## adicionar +H
  #### manha
  trabManha <- matriz2_bk %>% 
    dplyr::filter(COLABORADOR != "TIPO_DIA") %>% 
    dplyr::mutate(DATA = as.Date(DATA)) %>% 
    dplyr::group_by(DATA) %>% 
    dplyr::summarise(`+H` = sum(TIPO_TURNO=='M' & HORARIO=='H'),
                     TURNO = 'M', .groups='drop')
  #### tarde
  trabTarde <- matriz2_bk %>% 
    dplyr::filter(COLABORADOR != "TIPO_DIA") %>% 
    dplyr::mutate(DATA = as.Date(DATA)) %>% 
    dplyr::group_by(DATA) %>% 
    dplyr::summarise(`+H` = sum(TIPO_TURNO=='T' & HORARIO=='H'),
                     TURNO = 'T', .groups='drop')
  
  ##join +H com matrizB
  #### manha
  matrizB_M <- matrizB_ini %>% 
    dplyr::filter(TURNO == 'M') %>% 
    merge(trabManha, by=c("DATA","TURNO"), all.x = T) 
  
  #### tarde
  matrizB_T <- matrizB_ini %>% 
    dplyr::filter(TURNO == 'T') %>% 
    merge(trabTarde, by=c("DATA","TURNO"), all.x = T) 
  
  
  ##juntar manha e tarde -> matriz_B_final
  matrizB_ini <- matrizB_M %>% 
    dplyr::bind_rows(matrizB_T)
  
  ###apagar df auxs
  rm(matrizB_M,matrizB_T,trabManha,trabTarde)
  
  
  ##CALCULO DA FUNC OBJ + DIFF
  
  matrizB <- matrizB_ini %>% 
    dplyr::group_by(DATA,TURNO) %>%
    dplyr::mutate(maxTurno = as.numeric(maxTurno),
                  minTurno = as.numeric(minTurno)) %>% 
    dplyr::mutate(aux = replace_na(sdTurno/(maxTurno-minTurno),0)) %>% 
    ungroup() %>% 
    dplyr::mutate(pessObj = ceiling(ifelse(aux>=paramPessObj,maxTurno,mediaTurno)),
                  diff =  ceiling(`+H` - pessObj)) %>% data.table()
  
  
  #cria dia da semana
  matrizB$WDAY <- wday(as.Date(matrizB$DATA))
  matriB_bk <- matrizB
  
  
  return(list(matrizA_bk_OG=matrizA_bk_OG, matriz2_bk=matriz2_bk, matrizA_bk=matrizA_bk, matrizB_bk=matriB_bk, tiposDeTurno=tiposDeTurno))
  
}

#####Legacy funcTurnos useful is needed to go back
# funcTurnos <- function(matriz2, tipo) {
#   
#   matriz2_MoT <- matriz2 %>% 
#     dplyr::filter(TIPO_TURNO == tipo) %>% 
#     group_by(COLABORADOR, DATA) %>%
#     mutate(id = row_number())  %>% 
#     ungroup() %>% 
#     mutate(id=id-1)
#   
#   matriz2_MoT <- matriz2_MoT %>%
#     dplyr::mutate(TIPO_TURNO = case_when(
#       TIPO_TURNO == tipo & id == 1  ~ 'M',
#       TIPO_TURNO == tipo & id == 0  ~ 'T',
#       T ~ TIPO_TURNO
#     ))
#   
#   matriz2 <- bind_rows(matriz2 %>% 
#                          dplyr::filter(TIPO_TURNO != tipo), matriz2_MoT) %>% select(-id)
#   
#   return(matriz2)
#   
# }



funcTurnos<- function(matriz2, tipo){
  
  # Filter the rows that match 'tipo' and calculate 'id' in one step
  matriz2_MoT <- matriz2 %>%
    dplyr::filter(TIPO_TURNO == tipo) %>%
    group_by(COLABORADOR, DATA) %>%
    mutate(TIPO_TURNO = case_when(
      row_number() == 1 ~ 'M',   # First row becomes 'M'
      row_number() == 2 ~ 'T',   # Second row becomes 'T'
      TRUE ~ TIPO_TURNO          # Leave others unchanged
    )) %>%
    ungroup()
  
  # Combine the filtered and updated data with the rest of the data
  result <- matriz2 %>%
    dplyr::filter(TIPO_TURNO != tipo) %>%
    bind_rows(matriz2_MoT)
  
  return(result)
}

#matriz2 <- matriz2_bk
funcTipoTurnoFix<- function(matriz2){
  #find duplicate rows excluding column ID
  colnames_vec <- colnames(matriz2)
  colnames_vec <- colnames_vec[colnames_vec != "ID"]
  
  matriz2_fixed<- matriz2 %>% 
    group_by(across(all_of(colnames_vec))) %>% 
    dplyr::mutate(TIPO_TURNO = case_when(
                  n() >1 & TIPO_TURNO =='M' & row_number() == 2 ~ '0',
                  n() >1 & TIPO_TURNO =='T' & row_number() == 1 ~ '0',
                  TRUE ~ TIPO_TURNO
    )) %>% 
    ungroup()
  
  
  
  return(matriz2_fixed)
}
