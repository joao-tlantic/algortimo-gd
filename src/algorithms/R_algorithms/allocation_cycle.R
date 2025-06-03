# TODO: Implement loop logic to allocate descansos
# TODO: Remove the proccess errors: maybe return a dataframe containing running problems, then load them 
# function arguments should be R dataframes, with their names equal to what is defined in the python data container file, by the specific methods
source("path/to/helper_functions.R") # Source the needed functions

allocation_cycle <- function() {
  totalColabs <- length(unique(matrizA_bk$MATRICULA))
  logs <- NULL
  previous_colab <- ''
  contador <- 0
  brokeOff <- F
  # paramNLDF <- 2   #coloca NLDF (nao libraza aos domingos e feriados) 2 dias(DyF) antes e 2 dias(DyF) depois
  # time1 <- Sys.time()
  #OUT_PREVIOS
  matriz2_bk <- add_OUT(pathFicheirosGlobal,matriz2_bk, matrizA_bk, startDate2, endDate2, final)
  tryCatch({  
    #for (eachColab in unique(matrizA_bk$MATRICULA)) 
    while ((sum(matrizA_bk$L_TOTAL, na.rm = T))>0) {
      
      ###------SELECIONA COLABORADOR-------------
      # eachColab <-  unique(matrizA_bk$MATRICULA)[1]
      # tipoFolga <- 'L_DOM'
      
      # X0 <- load_m_equi_sab(colabsID, matrizB_bk, matrizA_bk, matriz2_bk)
      selectedColab <- selectColab(matrizA_bk,matrizEqui_domYfer,matrizEqui_sab)
      eachColab <- selectedColab[[1]]
      tipoFolga <- selectedColab[[2]]
      tipoFolga2 <- selectedColab[[3]]
      
      ### Check for infinite loops on the same colab
      if (eachColab == previous_colab) {
        contador <- contador + 1
        if (contador >= 200 && brokeOff == T) {
          #set_ProcessErrors(pathOS = pathFicheirosGlobal, user = wfm_user,
          #                  fk_process = wfm_proc_id,
          #                  type_error = 'A', process_type = 'GD_selectColab',
          #                  error_code = NA,
          #                  description = #paste0('1.9 Subproceso ',childNumber,' - error al obtener el calendario para puesto ',posto),
          #                    #setMessages(dfMsg,'errCalendar',list('1'=childNumber,'2'=posto)),
          #                    "erro loop infinito",
          #                  employee_id = NA, schedule_day = NA)
          #set_ProcessParamStatus(pathFicheirosGlobal, wfm_user, wfm_proc_id, 'I')
          q()

        }
      } else {
        contador <- 0
        previous_colab <- eachColab
      }
      print(paste('Colab:',eachColab))
      print(paste('Tipo folga:',tipoFolga))
      print(paste('folga2:',tipoFolga2))
      ###--- - - - -  -- - - - - - - - - - -  - - - - - -
      # if (eachColab %in% c('0155540') &
      #     tipoFolga=='L_RES'
      #     #tipoFolga=='C2D' | tipoFolga=='C3D'
      #     ) {
      #   print('break teste')
      #   break
      # }

      
      matriz2 <- matriz2_bk %>% 
        dplyr::filter(COLABORADOR == eachColab)
      
      matrizA <- matrizA_bk %>% 
        dplyr::filter(MATRICULA == eachColab)
      
      matrizB <- matrizB_bk
      
      matrizXor <- matrix()
      
      relaxaRegraDom <- F
      mudaDvizinhos <- 0
      mudaLvizinhos <- F
      relaxaMin <- F
      
      maxRelax <- max_Relax_og
    
      loop <- 0
      loopDyF <- 0
      criarMatrixLD <- T
      tipoLibranca <- 'NLD'
      # if (eachColab == '4064382') {
      #   break
      # }
      if (matrizA$TIPO_CONTRATO>3) {
        if (tipoFolga=='L_DOM') {
          paramNLDF <-  numFerDom/matrizA[[tipoFolga]]
          
          if (paramNLDF<=2) {
            paramNLDF <- 0
          } else{
            paramNLDF <- 2
          }
        }
        
      } else{
        paramNLDF <- 0
      }
      
      # ### VALIDA SE É PRECISO ATRIBUIR SABADOS
      if (!is.null(tipoFolga2) && tipoFolga2 == 'SABADO' && tipoFolga=='L_RES') {
        # print(paste('break',tipoFolga))
        # break

        if ((sum(matrizA[tipoFolga], na.rm = T))<=0) {
          matrizEqui_sab_colab <- matrizEqui_sab %>% 
            dplyr::filter(MATRICULA == eachColab) %>% 
            dplyr::mutate(Sab_atribuir = 0)
          
          matrizEqui_sab <- matrizEqui_sab %>% 
            dplyr::filter(MATRICULA != eachColab) %>% 
            dplyr::bind_rows(matrizEqui_sab_colab)
        }
      }
      it <- 0
      
      while ((sum(matrizA[tipoFolga], na.rm = T))>0) {
      #  if((loop > 0 || loopDyF > 0) && it > 0) break
        it <- 1
        print(paste('Colab:',eachColab))
        print(paste('Tipo folga:',tipoFolga))
        print(paste('folga2:',tipoFolga2))
        
        
        if (eachColab == previous_colab) {
          contador <- contador + 1
          print(paste('contador:',contador))
          if (contador >= 200) {
            brokeOff <- T

            if (nrow(matriz2 %>% dplyr::filter(DIA_TIPO=='domYf', HORARIO == 'OUT'))>0) {
              set_ProcessErrors(pathOS = pathFicheirosGlobal, user = wfm_user,
                                fk_process = wfm_proc_id,
                                type_error = 'A', process_type = 'GD_atribuiDescanso',
                                error_code = NA,
                                description = #paste0('1.9 Subproceso ',childNumber,' - error al obtener el calendario para puesto ',posto),
                                  #setMessages(dfMsg,'errCalendar',list('1'=childNumber,'2'=posto)),
                                  paste("imposible asignar todos los domingos para colab:", eachColab," debido a OUT"),
                                employee_id = NA, schedule_day = NA)
            } else{
              set_ProcessErrors(pathOS = pathFicheirosGlobal, user = wfm_user,
                                fk_process = wfm_proc_id,
                                type_error = 'E', process_type = 'GD_atribuiDescanso',
                                error_code = NA,
                                description = #paste0('1.9 Subproceso ',childNumber,' - error al obtener el calendario para puesto ',posto),
                                  #setMessages(dfMsg,'errCalendar',list('1'=childNumber,'2'=posto)),
                                  #paste("erro loop infinito para colab:", eachColab," tipo_folga: ",tipoFolga),
                                paste("No fue posible asignar todos los descansos ",tipoFolga," al empleado ", eachColab),
                                
                                employee_id = NA, schedule_day = NA)
            }

            matrizA[tipoFolga] <- -as.numeric(matrizA[tipoFolga])
            matrizA$L_TOTAL <- matrizA$L_TOTAL+as.numeric(matrizA[tipoFolga])
            
            matrizA_bk <- matrizA_bk %>%
              dplyr::filter(MATRICULA != eachColab) %>%
              dplyr::bind_rows(matrizA)
            next
            # set_ProcessParamStatus(pathFicheirosGlobal, wfm_user, wfm_proc_id, 'I')
            # q()

          }
        } else {
          contador <- 0
          previous_colab <- eachColab
        }
      ###------ SELECIONA SEMANA ----------
      semanaSelecionada <- NULL
      colabSelecionado <- eachColab
      
      
      if (tipoFolga=='L_D' & (sum(matrizA$L_DOM, na.rm = T))<=0 & criarMatrixLD==T & (sum(matrizA$L_D, na.rm = T))>0) {

        #cria matrizXOR
        matrizXor <- criarMatrizXor(matriz2)
        
        LD_old <- matrizA$L_D
        LD_new <- matriz2 %>% 
          dplyr::arrange(COLABORADOR, DATA, desc(HORARIO)) %>%
          dplyr::group_by(COLABORADOR, DATA) %>%
          dplyr::mutate(dups = row_number()) %>% ungroup() %>% 
          dplyr::filter(dups==1) %>% 
          dplyr::group_by(COLABORADOR) %>% 
          dplyr::summarise(xx = sum(DIA_TIPO=='domYf' & HORARIO %in% c('OUT','H','NLDF','DFS'))) %>% .$xx
        
        LD_diff <- LD_old - LD_new
        
        # if (LD_diff>0) {
          matrizA$L_D <- LD_new#min(LD_old,LD_new)
          matrizA$L_TOTAL <- matrizA$L_TOTAL - LD_diff
          
          matrizA_bk <- matrizA_bk %>%
            dplyr::filter(MATRICULA != eachColab) %>%
            dplyr::bind_rows(matrizA)
        # }
       

        if (is.null(matrizXor)) {
          print("inicialmente ja nao tem espaço para L_D ")
          matrizA$L_D <- matrizA$L_D*-1
          matrizA$L_TOTAL <- matrizA$L_TOTAL+matrizA$L_D

          matrizA_bk <- matrizA_bk %>%
            dplyr::filter(MATRICULA != eachColab) %>%
            dplyr::bind_rows(matrizA)


          matrizXor <- matrix()

          if ((sum(matrizA$L_TOTAL, na.rm = T))<=0) {
            print("e nao tem mais folgas a dar")
              break
          }
        }
        #desliga param para nao voltar a criar
        criarMatrixLD <- F
      }
    
  
      ###------ SELECIONA DIA-TURNO--------
      # relaxaMin <- T
      print("procura dia")
      # matrizA2 <- matrizA
      # matrizB2 <- matrizB
      # matriz22 <- matriz2
      # matrizA <- matrizA2
      # matrizB <- matrizB2
      # matriz2 <- matriz22
      matriz_data_turno <- selectDayShift(matrizA, matrizB, matriz2, mudaLvizinhos,
                                          relaxaRegraDom, relaxaMin,semanasTrabalho, 
                                          semanaSelecionada, colabSelecionado,matriz2_bk,
                                          mudaDvizinhos,convenio,matrizXor,maxRelax, tipoFolga, tipoFolga2)
      matriz_data_turno_bk <- matriz_data_turno
      
     
    
      ### relaxa parametros para tentar atribuir domingos------------------
      if (nrow(matriz_data_turno)==0  &&  (nrow(matrizA %>% dplyr::filter(L_DOM >0))>0)) {
          print("domingos em falta ---- limite atingido")
          # paramNLDF <- 0
        
          loopDyF <- loopDyF + 1
          # print(partir)
          if (loopDyF == 1) {
            mudaDvizinhos <- 1
          } else if (loopDyF == 2) {
            
            mudaDvizinhos <- 2
            # print(break_teste)
          } else if (loopDyF == 3 & is.null(tipoFolga2)) {
            relaxaMin <- T
          } else if (loopDyF == 4 & is.null(tipoFolga2) #& max_Relax_og==T
                     ) {
            maxRelax <- T
          } else{
            paramNLDF <- 0
            if (!is.null(tipoFolga2) && tipoFolga2 == 'L_DOM_TARDE') {
              print("nao consigo atribuir mais L_DOM_TARDE")
              # break
              relaxaRegraDom <- F
              mudaDvizinhos <- 0
              mudaLvizinhos <- F
              relaxaMin <- F
              
              maxRelax <- max_Relax_og
              
              loop <- 0
              loopDyF <- 5
              criarMatrixLD <- T
              tipoLibranca <- 'NLD'
              tipoFolga2 <- NULL
              
              matriz2 <- matriz2 %>%
                dplyr::mutate(HORARIO = case_when(
                  TIPO_TURNO == 'T' & DIA_TIPO == 'domYf' & HORARIO == 'H' ~ 'NLDF',
                  T ~ HORARIO
                ))
              
              matriz2_bk <- matriz2_bk %>%
                dplyr::filter(COLABORADOR != eachColab) %>%
                dplyr::bind_rows(matriz2)
            } else if (!('H' %in% unique(matriz2 %>% dplyr::filter(DIA_TIPO=='domYf') %>% .$HORARIO))) { ### e H é domYF
              print(paste(eachColab,"LOOP SEM ESPACO"))
              logs <- c(logs,paste(eachColab,"LOOP SEM ESPACO"))
              paramNLDF <- 0
              
              if (loopDyF <= 7){
                #loopDyF <- loopDyF-1
                if(nrow(matriz2 %>% dplyr::filter(DIA_TIPO=='domYf', TIPO_TURNO %in% c('M') ,HORARIO == 'NLDF'))>0){
                  matriz2 <- matriz2 %>%
                    dplyr::mutate(HORARIO = case_when(
                      TIPO_TURNO == 'M' & DIA_TIPO == 'domYf' & HORARIO == 'NLDF' ~ 'H',
                      T ~ HORARIO
                    ))
                } else {#if (nrow(matriz2 %>% dplyr::filter(DIA_TIPO=='domYf', TIPO_TURNO %in% c('T') ,HORARIO == 'NLDF'))>0)
                  matriz2 <- matriz2 %>%
                    dplyr::mutate(HORARIO = case_when(
                      DIA_TIPO == 'domYf' & HORARIO == 'NLDF' ~ 'H',
                      T ~ HORARIO
                    ))
                }
                
                
                matriz2_bk <- matriz2_bk %>%
                  dplyr::filter(COLABORADOR != eachColab) %>%
                  dplyr::bind_rows(matriz2)
              } else{
                
                
                if (nrow(matriz2 %>% dplyr::filter(DIA_TIPO=='domYf', HORARIO == 'OUT'))>0) {
                  
                  paramNLDF <- 0
                  relaxaRegraDom <- F
                  mudaDvizinhos <- 0
                  mudaLvizinhos <- F
                  relaxaMin <- F
                  
                  maxRelax <- max_Relax_og
                  
                  loop <- 0
                  loopDyF <- 0
                  criarMatrixLD <- T
                  tipoLibranca <- 'NLD'
                  tipoFolga2 <- NULL
                  
                  matriz2 <- matriz2 %>%
                    dplyr::mutate(HORARIO = case_when(
                      TIPO_TURNO %in% c('M','T') & DIA_TIPO == 'domYf' & HORARIO == 'OUT' ~ 'H',
                      T ~ HORARIO
                    ))
                  
                  matriz2_bk <- matriz2_bk %>%
                    dplyr::filter(COLABORADOR != eachColab) %>%
                    dplyr::bind_rows(matriz2)
                  # set_ProcessErrors(pathOS = pathFicheirosGlobal, user = wfm_user,
                  #                    fk_process = wfm_proc_id,
                  #                    type_error = 'A', process_type = 'GD_atribuiDescanso',
                  #                    error_code = NA,
                  #                    description = #paste0('1.9 Subproceso ',childNumber,' - error al obtener el calendario para puesto ',posto),
                  #                      #setMessages(dfMsg,'errCalendar',list('1'=childNumber,'2'=posto)),
                  #                      paste("imposible asignar todos los domingos para colab:", eachColab," debido a OUT"),
                  #                    employee_id = NA, schedule_day = NA)
                } else{
                  matrizA <- matrizA %>%
                    dplyr::mutate(L_TOTAL = L_TOTAL - L_DOM,
                                  L_DOM = - L_DOM)
                  matrizA_bk <- matrizA_bk %>%
                    dplyr::filter(MATRICULA != eachColab) %>%
                    dplyr::bind_rows(matrizA)
                }
                
                
              }
              
              
              next
            } else if (('NLDF' %in% unique(matriz2 %>% dplyr::filter(DIA_TIPO=='domYf') %>% .$HORARIO))) {
              print(paste(eachColab,"LOOP SEM ESPACO POR ESTIMATIVAS"))
              logs <- c(logs,paste(eachColab,"LOOP SEM ESPACO POR ESTIMATIVAS"))
              paramNLDF <- 0
              
              if (loopDyF <= 7){
                matriz2 <- matriz2 %>%
                  dplyr::mutate(HORARIO = case_when(
                    DIA_TIPO == 'domYf' & HORARIO == 'NLDF' ~ 'H',
                    T ~ HORARIO
                  ))
                
                matriz2_bk <- matriz2_bk %>%
                  dplyr::filter(COLABORADOR != eachColab) %>%
                  dplyr::bind_rows(matriz2)
              } else{
                matrizA <- matrizA %>%
                  dplyr::mutate(L_TOTAL = L_TOTAL - L_DOM,
                                L_DOM = - L_DOM)
                
                if (nrow(matriz2 %>% dplyr::filter(DIA_TIPO=='domYf', HORARIO == 'OUT'))>0) {
                  set_ProcessErrors(pathOS = pathFicheirosGlobal, user = wfm_user,
                                    fk_process = wfm_proc_id,
                                    type_error = 'A', process_type = 'GD_atribuiDescanso',
                                    error_code = NA,
                                    description = #paste0('1.9 Subproceso ',childNumber,' - error al obtener el calendario para puesto ',posto),
                                      #setMessages(dfMsg,'errCalendar',list('1'=childNumber,'2'=posto)),
                                      paste("imposible asignar todos los domingos para colab:", eachColab," debido a OUT_2"),
                                    employee_id = NA, schedule_day = NA)
                }
                
                matrizA_bk <- matrizA_bk %>%
                  dplyr::filter(MATRICULA != eachColab) %>%
                  dplyr::bind_rows(matrizA)
              }
              
            } else if (nrow(matriz2 %>% dplyr::filter(DIA_TIPO=='domYf', HORARIO == 'OUT'))>0) {
              
              paramNLDF <- 0
              relaxaRegraDom <- F
              mudaDvizinhos <- 0
              mudaLvizinhos <- F
              relaxaMin <- F
              
              maxRelax <- max_Relax_og
              
              loop <- 0
              loopDyF <- 0
              criarMatrixLD <- T
              tipoLibranca <- 'NLD'
              tipoFolga2 <- NULL
              
              matriz2 <- matriz2 %>%
                dplyr::mutate(HORARIO = case_when(
                  TIPO_TURNO %in% c('M','T') & DIA_TIPO == 'domYf' & HORARIO == 'OUT' ~ 'H',
                  T ~ HORARIO
                ))
              
              matriz2_bk <- matriz2_bk %>%
                dplyr::filter(COLABORADOR != eachColab) %>%
                dplyr::bind_rows(matriz2)
              # set_ProcessErrors(pathOS = pathFicheirosGlobal, user = wfm_user,
              #                    fk_process = wfm_proc_id,
              #                    type_error = 'A', process_type = 'GD_atribuiDescanso',
              #                    error_code = NA,
              #                    description = #paste0('1.9 Subproceso ',childNumber,' - error al obtener el calendario para puesto ',posto),
              #                      #setMessages(dfMsg,'errCalendar',list('1'=childNumber,'2'=posto)),
              #                      paste("imposible asignar todos los domingos para colab:", eachColab," debido a OUT"),
              #                    employee_id = NA, schedule_day = NA)
            } else{
              matrizA <- matrizA %>%
                dplyr::mutate(L_TOTAL = L_TOTAL - L_DOM,
                              L_DOM = - L_DOM)
              matrizA_bk <- matrizA_bk %>%
                dplyr::filter(MATRICULA != eachColab) %>%
                dplyr::bind_rows(matrizA)
              
              set_ProcessErrors(pathOS = pathFicheirosGlobal, user = wfm_user,
                                fk_process = wfm_proc_id,
                                type_error = 'A', process_type = 'GD_atribuiDescanso',
                                error_code = NA,
                                description = #paste0('1.9 Subproceso ',childNumber,' - error al obtener el calendario para puesto ',posto),
                                  #setMessages(dfMsg,'errCalendar',list('1'=childNumber,'2'=posto)),
                                  paste("imposible asignar todos los domingos para colab:", eachColab),
                                employee_id = NA, schedule_day = NA)
            }
            
          }
          
          relaxaRegraDom <- T
          
          next
      }
      
      
      ### relaxa parametros para tentar restantes folgas------------------
      if (nrow(matriz_data_turno)==0 ) {
        print("folgas com limite atingido")
        # paramNLDF <- 0
        loop <- loop+1
        if (loop == 1) {
          relaxaMin <- T
          
        }else if (loop == 2) {
          maxRelax <- T
          
        } else{
          

          if (tipoFolga=='L_D') { ### e H é domYF
            logs <- c(logs,paste(eachColab,"LOOP SEM ESPACO para LD"))
            # print(validarLDsCoisas)
            matrizA <- matrizA %>%
              dplyr::mutate(L_TOTAL = L_TOTAL - L_D,
                            L_D = 0)
            matrizA_bk <- matrizA_bk %>%
              dplyr::filter(MATRICULA != eachColab) %>%
              dplyr::bind_rows(matrizA)
            # next
          } else if (!is.null(tipoFolga2) && tipoFolga2 == 'SABADO' && tipoFolga=='L_RES') {

            #mudaLvizinhos <- F
            relaxaMin <- F
            maxRelax <- max_Relax_og
            
            loop <- 0
            tipoFolga2 <- NULL
            
            next
          }else if (tipoFolga=='C2D') {
            logs <- c(logs,paste(eachColab,"LOOP SEM ESPACO para C2D"))
            
            matrizA <- matrizA %>%
              dplyr::mutate(L_TOTAL = L_TOTAL - C2D,
                            C2D = 0)
            matrizA_bk <- matrizA_bk %>%
              dplyr::filter(MATRICULA != eachColab) %>%
              dplyr::bind_rows(matrizA)
            # next
          }else if (tipoFolga=='C3D') {
            logs <- c(logs,paste(eachColab,"LOOP SEM ESPACO para C3D"))
            
            matrizA <- matrizA %>%
              dplyr::mutate(L_TOTAL = L_TOTAL - C3D,
                            C3D = 0)
            matrizA_bk <- matrizA_bk %>%
              dplyr::filter(MATRICULA != eachColab) %>%
              dplyr::bind_rows(matrizA)
            # next
          }else if (tipoFolga=='C3D') {
            logs <- c(logs,paste(eachColab,"LOOP SEM ESPACO para C3D"))
            
            matrizA <- matrizA %>%
              dplyr::mutate(L_TOTAL = L_TOTAL - C3D,
                            C3D = 0)
            matrizA_bk <- matrizA_bk %>%
              dplyr::filter(MATRICULA != eachColab) %>%
              dplyr::bind_rows(matrizA)
            # next
          }else if (tipoFolga=='L_Q') {
            logs <- c(logs,paste(eachColab,"LOOP SEM ESPACO para L_Q"))
            
            matrizA <- matrizA %>%
              dplyr::mutate(L_TOTAL = L_TOTAL - L_Q,
                            L_Q = 0)
            matrizA_bk <- matrizA_bk %>%
              dplyr::filter(MATRICULA != eachColab) %>%
              dplyr::bind_rows(matrizA)
            # next
          }
          # else{
          #   PARA OUTROS TIPOS DE FOLGA APLICAR A MESMA LOGICA
          # }
          matrizA_bk <- matrizA_bk %>%
            mutate_all(~replace(., . < 0, 0))
          
          set_ProcessErrors(pathOS = pathFicheirosGlobal, user = wfm_user,
                            fk_process = wfm_proc_id,
                            type_error = 'A', process_type = 'GD_atribuiDescanso',
                            error_code = NA,
                            description = #paste0('1.9 Subproceso ',childNumber,' - error al obtener el calendario para puesto ',posto),
                              #setMessages(dfMsg,'errCalendar',list('1'=childNumber,'2'=posto)),
                              paste("No fue posible asignar todos los descansos ",tipoFolga," al empleado ", eachColab),
                            employee_id = NA, schedule_day = NA)
        }
        
        next
      }
    
      diaTurnoSelecionado <- matriz_data_turno %>% 
        dplyr::select(DATA,TURNO) %>% unique() %>% 
        dplyr::mutate(DATA = as.character(DATA))
      
      tipoLibranca <- unique(matriz_data_turno$COLUNA)
    
      
      ###------ATRIBUI DESCANSO-------------
      
      print("atribui1 descanso")
      print(colabSelecionado)
      print(diaTurnoSelecionado$DATA)
      
      ## APENAS PARA VER OS DOMINGOS A TARDE MARCADOS PELO FORCE
      if (!is.null(tipoFolga2) && tipoFolga2 == 'L_DOM_TARDE') {
        tipoLibranca <- tipoFolga2
      }
  
      # if (matriz2 %>% 
      #     dplyr::group_by(COLABORADOR, DATA) %>%
      #     dplyr::summarise(diasH = sum(any(grepl('H|NL',HORARIO , ignore.case = TRUE))),
      #                      nlOut = sum(any(HORARIO=='OUT')),
      #                      nlDFS = sum(any(HORARIO=='DFS')),
      #                      .groups='drop') %>% 
      #     dplyr::mutate(diasH =  diasH + nlOut + nlDFS) %>% 
      #     dplyr::mutate(WW = isoweek(DATA)) %>%
      #     dplyr::filter(WW == isoweek(matriz_data_turno$DATA)) %>% 
      #     dplyr::group_by(COLABORADOR, WW) %>%
      #     dplyr::summarise(diasH = sum(diasH), .groups='drop') %>% .$diasH < 5) {
      #   print(pipipparou)
      # }
      
      result <- atribuiDesc(colabSelecionado, diaTurnoSelecionado, matriz2_bk, matrizA_bk, matrizB, tipoLibranca, paramNLDF,paramNL10)
      
      neg <- result$MA %>%
        dplyr::filter(MATRICULA == eachColab ,L_DOM <0)
      # neg <- (result$MA %>%
      #     dplyr::filter(MATRICULA == eachColab))[tipoLibranca][[1]]
      if (nrow(neg)>0 & tipoLibranca %in% c("L_DOM", "L_DOM_TARDE")) {
        print("negativo dom")
        break
      }
      
      # abc <- result$M2 %>% 
      #   dplyr::filter(DATA == diaTurnoSelecionado$DATA & TIPO_TURNO == diaTurnoSelecionado$TURNO) %>% 
      #   dplyr::arrange(COLABORADOR, DATA, desc(HORARIO)) %>%
      #   dplyr::group_by(COLABORADOR, DATA) %>%
      #   dplyr::mutate(dups = row_number()) %>% ungroup() %>% 
      #   dplyr::filter(dups==1) %>% 
      #   dplyr::group_by(DATA) %>% 
      #   dplyr::summarise(xx = sum(HORARIO!='L_DOM' & grepl('C|L_',HORARIO , ignore.case = TRUE))) %>%
      #   dplyr::filter(xx>=2) %>% nrow()
      # if (abc>0 & tipoLibranca!='L_DOM') {
      #   print("atribui mais do que o que devia neste dia")
      #   print(pararrr)
      # }
      
      matriz2_bk <- result$M2
      matriz2_bk$DATA <- as.character(matriz2_bk$DATA)
      matrizA_bk <- result$MA
      matrizB <- result$MB
      matrizB_bk <- matrizB
      print("atribui2 descanso")
      
      matrizEqui_domYfer <- loadMEqui(colabsID, matrizB_bk, matrizA_bk, matrizA_og, matriz2_bk)
      
      matrizEqui_sab <- load_m_equi_sab(colabsID, matrizB_bk, matrizA_bk, matriz2_bk)
      
      # ### VALIDA SE É PRECISO ATRIBUIR MAIS SABADOS
      if (!is.null(tipoFolga2) && tipoFolga2 == 'SABADO' && tipoFolga=='L_RES') {
        # print(paste('break',tipoFolga))
        # break
        if ((matrizEqui_sab %>% dplyr::filter(MATRICULA == eachColab) %>% .$Sab_atribuir)<0) {
          print("FOLGA L_RES, TENTO ATRIBUIR SABADOS")
          tipoFolga2 <- NULL
        }
      }
      
      
      matriz2 <- matriz2_bk %>% 
        dplyr::filter(COLABORADOR == eachColab) #%>% 
        # dplyr::bind_rows(matriz2)
      
      matrizA <- matrizA_bk %>% 
        dplyr::filter(MATRICULA == eachColab) #%>% 
        # dplyr::bind_rows(matrizA)
    
      ###------FIM DA FUNC ATRIBUI DESCANSO-------------
      
      # if ((sum(matrizA$LQ, na.rm = T))==0) {
      #   matrizA <- matrizA %>% 
      #     dplyr::mutate(L_RES = L_TOTAL)
      # }
      
      
      #### ATUALIZA MATRIZ SEMANAS NORMAL--------------------------------------------------
      semanasTrabalho <- matriz2_bk %>% 
        dplyr::group_by(COLABORADOR, DATA) %>%
        dplyr::summarise(diasH = sum(any(grepl('H|NL',HORARIO , ignore.case = TRUE))),
                         nlOut = sum(any(HORARIO=='OUT')),
                         nlDFS = sum(any(HORARIO=='DFS')),
                         .groups='drop') %>% 
        dplyr::mutate(diasH =  diasH + nlOut + nlDFS) %>% 
        dplyr::mutate(WW = isoweek(DATA)) %>% 
        dplyr::group_by(COLABORADOR, WW) %>%
        dplyr::summarise(diasH = sum(diasH), .groups='drop') %>% 
        merge(matrizA_bk %>% select(MATRICULA,TIPO_CONTRATO), by.x = 'COLABORADOR', by.y = 'MATRICULA') %>% 
        #reshape2::dcast( COLABORADOR+TIPO_CONTRATO ~ WW, value.var = "diasH") %>% View()
        dplyr::mutate(nTrab = ifelse(WW==53,2-diasH,7 - diasH),
                      delta = diasH - 
                        as.numeric(
                          ifelse(
                            grepl('H',TIPO_CONTRATO , ignore.case = TRUE) ,
                            5,
                            TIPO_CONTRATO
                          )
                        )
        )  
      
      semanasTotal <- semanasTrabalho %>% 
        dplyr::group_by(WW) %>%
        dplyr::summarise(delta = sum(delta), .groups='drop') %>% ungroup()
      
      colabTotal <- semanasTrabalho %>% 
        dplyr::group_by(COLABORADOR) %>%
        dplyr::summarise(delta = sum(delta), .groups='drop') %>% ungroup()
      
      
      #remove semana LD da matrizXor------------
      if (tipoLibranca=='L_D' && sum(matrizXor)>0) {
        
        selWW <- isoweek(diaTurnoSelecionado$DATA)
        
        totalWW <- colSums(matrizXor > 0, na.rm = TRUE)[[selWW]]
        
        if (totalWW==1) {
          # Find the indices of the first occurrence of the value in the entire matrix
          indices <- which(matrizXor == isoweek(diaTurnoSelecionado$DATA), arr.ind = TRUE)[1, "row"]
          # Replace the first occurrence with 0
          if (!is.na(indices[[1]])) {
            row <- unique(indices[[1]])
            matrizXor[row,] <- 0
          }
        } else{
          #extrai valores da semana escolhida
          vals <- unique(c(matrizXor[, isoweek(diaTurnoSelecionado$DATA)]))
          #filtrar valor >0, junta "nome" da semana
          vals <- unique(c(vals[vals>0],isoweek(diaTurnoSelecionado$DATA)))
          
          ##valida se semana anterior tem 7dias de trabalho
          diasH <- semanasTrabalho %>% 
            dplyr::filter(COLABORADOR == eachColab, WW == min(vals)) %>% .$diasH
          
          if (diasH==7) {
            # Find the indices of the first occurrence of the value in the entire matrix
            indices <- which(matrizXor == isoweek(diaTurnoSelecionado$DATA), arr.ind = TRUE)[2, "row"]
            # Replace the first occurrence with 0
            if (!is.na(indices[[1]])) {
              row <- unique(indices[[1]])
              matrizXor[row,] <- 0
            }
          } else{
            # Find the indices of the first occurrence of the value in the entire matrix
            if (any(matrizXor == isoweek(diaTurnoSelecionado$DATA))) {
              indices <- which(matrizXor == isoweek(diaTurnoSelecionado$DATA), arr.ind = TRUE)[1, "row"]
              # Replace the first occurrence with 0
              if (!is.na(indices[[1]])) {
                row <- unique(indices[[1]])
                matrizXor[row,] <- 0
              }
            }

          }
        }
        
        
        
      }
      
      }
      
      # if (eachColab %in% c('0151601','5009554','5020391')#tipoFolga=='L_DOM' 
      #         ) {
      #       print('break teste')
      #       break
      #     }
      
      if (tipoFolga=='L_DOM' & (sum(matrizA$L_DOM, na.rm = T))<=0) {
        cleanSundays <- getInfoRemove(matriz2, matrizA$MATRICULA)
        matriz2_bk <- matriz2_bk %>% 
          dplyr::filter(COLABORADOR != eachColab) %>% 
          dplyr::bind_rows(cleanSundays)
      }
    
    }
    
    time2 <- Sys.time()
    
    print(time2- time1)
  }, error = function(e){
    set_ProcessErrors(pathOS = pathFicheirosGlobal, user = wfm_user, 
                      fk_process = wfm_proc_id, 
                      type_error = 'E', process_type = 'GD_atribuiDescanso', 
                      error_code = NA, 
                      description = #paste0('1.9 Subproceso ',childNumber,' - error al obtener el calendario para puesto ',posto),
                        #setMessages(dfMsg,'errCalendar',list('1'=childNumber,'2'=posto)),
                        "error al asignar descanso",
                      employee_id = NA, schedule_day = NA)
    set_ProcessParamStatus(pathFicheirosGlobal, wfm_user, wfm_proc_id, 'I')
    q()
  })
}